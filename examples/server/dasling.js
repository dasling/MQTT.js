var mqtt = require('../..')
  , util = require('util')
  , config = require('./config')
  , Dbclient = require('mariasql') // https://github.com/mscdex/node-mariasql
  , db_util = require('./db_util');

// setup the DB connection
var dbclient = new Dbclient();
var db_config = config.getConfiguration().database;
dbclient.connect(db_config);

// log the events on the DB connection
dbclient.on('connect', function() {
   console.log('DB Client connected');
 })
 .on('error', function(err) {
   console.log('DB Client error: ' + err);
 })
 .on('close', function(hadError) {
   console.log('DB Client closed');
 });
 
var myMQTTServer = mqtt.createServer(function(client) { 
  // this function is called on a new connection (emiting also "client" event)
  // client argument is a "new Connection (socket, MqttServer)"

  var self = this;

  if (!self.clients) self.clients = {}; 

  client.on('connect', function(packet) {
        //Connack return codes are:
        //0 0x00 Connection Accepted
        //1 0x01 Connection Refused: unacceptable protocol version
        //2 0x02 Connection Refused: identifier rejected
        //3 0x03 Connection Refused: server unavailable
        //4 0x04 Connection Refused: bad user name or password
        //5 0x05 Connection Refused: not authorized
        //6-255  Reserved for future use 

    // TODO: If the client sends an invalid CONNECT message, the server should close the connection. This includes CONNECT 
    // messages that provide invalid Protocol Name or Protocol Version Numbers. If the server can parse enough of the 
    // CONNECT message to determine that an invalid protocol has been requested, it may try to send a CONNACK containing 
    // the "Connection Refused: unacceptable protocol version" code before dropping the connection.

    // Check the device authorization
    if (packet.username && packet.password && packet.clientId) {
      
      // log connection to DB
      db_util.log(packet, "CONNECT: Client " + packet.clientId + " connected with credentials: " + client.username + " / " + client.password, dbclient);

      var preproc_sql = dbclient.prepare('SELECT device_id FROM device_auth WHERE client_id = :id AND username = :username AND password = :password AND status_id = 1');

      dbclient.query(preproc_sql({id: packet.clientId, username: packet.username, password: packet.password }))
        .on('result', function(res) {
	  res.on('row', function(row) { // Device is authorized
            db_util.log(packet, 'Connect from authorized device: ' + util.inspect(row));
            // Store information inside the client object
            client.authorized = true;
            client.username = packet.username;
            client.password = packet.password;
            client.id = packet.clientId;
	  })
          .on('error', function(err) {
            console.log('Result error: ' + util.inspect(err));
          })
          .on('end', function(info) {
            if (info.numRows > 1) {
              // This is not necessarily a problem, but only the last device retrieved with these credentials will be stored
              db_util(packet, 'ClientId, username, password has multiple devices', dbclient);
            }
            if (info.numRows < 1) {
              // Device is not authorized
              db_util.log(util.inspect(packet), '*** WARNING *** CONNECT: Received a connect packet with a bad authorization (clientId, username, password combo). Sending connack with return code 4 (bad username or password)', dbclient);
              client.connack({returnCode: 4});  // Bad username or password
            }
            if (info.numRows == 1) {
              // TODO: Check for the client id, If a client with the same Client ID is already connected to the server, 
              // the "older" client must be disconnected (e.g. client.stream.end() ) by the server before completing the 
              // CONNECT flow of the new client.
              client.subscriptions = [];      	// Set up the empty subscriptions array for this client
              // Store the client in a associative array
              self.clients[client.id] = client;
              client.connack({returnCode: 0});      
            }
          });
       })
       .on('end', function() {
       });
    }
    else {
      db_util.log(util.inspect(packet), 'CONNECT: Received a connect packet missing a username or a password. Sending connack with return code 4 (bad username or password)', dbclient);
      client.connack({returnCode: 4});  // Bad username or password
    }
  });

  client.on('subscribe', function(packet) {
    // double-check for device authorization
    if (client.authorized != true) {
      // MQTT 3.1 says we can't send anything to say that subscribe not denied
      return;
    }
    
    var granted = [];

    for (var i = 0; i < packet.subscriptions.length; i++) {
      var qos = packet.subscriptions[i].qos
        , topic = packet.subscriptions[i].topic
        , reg = new RegExp(topic.replace('+', '[^\/]+').replace('#', '.+$'));

      granted.push(qos);
      client.subscriptions.push(reg);
    }

    client.suback({messageId: packet.messageId, granted: granted}); // Granted are the granted QoS'es
  });

  client.on('publish', function(packet) {    // TODO: Rewrite this with the async library (parallel execution path)
    // Sanitize the input/payload
    console.log("***********************");
    console.log("PUBLISH received: client id: "  + client.id + ", payload: " + packet.payload + ", topic: " + packet.topic);
    
    // TODO: should the client id be checked (can it publish if not "connected"?)
    
    // store in database when topic ends with "gauge"
    if ((new RegExp('^\/sensor')).test(packet.topic) | (new RegExp('^\/actuator')).test(packet.topic) | (new RegExp('^\/algo')).test(packet.topic)) {
      // convert the payload to an array
      var packet_payload = packet.payload.slice(1,-1).split(','); 	// payload now equals [ '1368630597', '0', '"W"' ]
      packet_payload[2] = packet_payload[2].slice(1,-1); 		// payload now equals [ '1368630597', '0', 'W' ]
      console.log("Payload/Datetime: " + packet_payload[0] + ", Payload/Value: " + packet_payload[1] + ", Payload/Unit: " + packet_payload[2]);
      
      // TODO: Check the unit (is it correctly set in the DB?), maybe we only want to do this once? caching the result thus.

      // Retrieve channel_setup_id from PUBLISH
      // var channel_user_given_id_from_topic = packet.topic.match(/^\/sensor\/([\w]+)\/gauge$/)[1];
      var channel_user_given_id_from_topic = packet.topic;
      
      // console.log("Channel_user_given_id from received packet: " + channel_user_given_id_from_topic);
      var channel_id_from_DB;
     
      var preproc_sql = dbclient.prepare('SELECT a.client_id AS client_id, c.channel_id AS channel_id FROM channels c \
                                            JOIN devices d \
                                              ON c.device_id = d.device_id AND c.organization_id = d.organization_id \
                                            JOIN device_auth a \
                                              on d.device_id = a.device_id AND d.organization_id = a.organization_id \
                                          WHERE c.channel_user_given_id = :channel_user_given_id \
                                            AND c.organization_id = :organization_id \
                                            AND a.client_id = :client_id \
                                          LIMIT 1');
      dbclient.query(preproc_sql({organization_id : 1, 
				  channel_user_given_id : channel_user_given_id_from_topic,
                                  client_id: client.id}))
      .on('result', function(res) {
	res.on('row', function(row) {
	  console.log('Result row: ' + util.inspect(row));
	  // Store the channel_id
          channel_id_from_DB = row.channel_id;
          console.log('Channel id (in DB): ' + channel_id_from_DB);
          
        })
	.on('error', function(err) {
	  console.log('Result error: ' + util.inspect(err));
	})
	.on('end', function(info) {
          // console.log('Result finished successfully');
          if (info.numRows != 1) {
            // TODO: log this
            // console.log("Client_id " + client.id + "not authorized for channel " + channel_user_given_id_from_topic);
          } else {
            //console.log("Client_id " + client.id + " authorized for channel " + channel_user_given_id_from_topic);

            // Storing the data in the DB
            var preproc_sql = dbclient.prepare(
              'INSERT INTO readings (organization_id, measured_at_timestamp) ' 
              + 'VALUES (:organization_id, :measured_at_timestamp)');
            dbclient.query(preproc_sql({organization_id : 1, 
                                        measured_at_timestamp : packet_payload[0]}))
            .on('result', function(res) {
              res.on('row', function(row) {
                // console.log('Result row: ' + util.inspect(row));
              })
              .on('error', function(err) {
                console.log('Result error: ' + util.inspect(err));
              })
              .on('end', function(info) {
                // console.log('Result finished successfully');
                if (info.affectedRows == 1) { // 1 reading was added
                  var republish_topic;
                  // Get the variable_id currently used for this channel (TODO: need to come for a query, not from the user's packet)
                  var current_variable_for_this_channel; 
                  var preproc_sql = dbclient.prepare('SELECT v.variable_id, v.republish_topic from variable v \
                                                        WHERE v.organization_id = :organization_id \
                                                          AND v.current_channel_id = :channel_id_from_DB \
                                                        LIMIT 1');
                  dbclient.query(preproc_sql({organization_id : 1, 
                                              channel_id_from_DB: channel_id_from_DB}))
                  .on('result', function(res) {
                    res.on('row', function(row) {
                      // console.log('Result row: ' + util.inspect(row)); // a variable is declared for this channel
                      current_variable_for_this_channel = row.variable_id;
                      republish_topic = row.republish_topic;
                    })
                    .on('error', function(err) {
                      console.log('Result error: ' + util.inspect(err));
                    })
                    .on('end', function(info) {
                      // console.log('Result finished successfully');
                      if (info.numRows == 1) { // TODO: log error & stop_processing this message (break)
                        
                        // reading_id
                        var reading_id = info.insertId;
                        
                        // Now add the values for this reading
                        var preproc_sql = dbclient.prepare(
                            'INSERT INTO value (organization_id, reading_id, variable_id, channel_id, value) ' 
                            + 'VALUES (:organization_id, :reading_id, :variable_id, :channel_id, :value)');
                        dbclient.query(preproc_sql({organization_id : 1, 
                                                    reading_id : reading_id,
                                                    variable_id : current_variable_for_this_channel, 
                                                    channel_id : channel_id_from_DB,
                                                    value : packet_payload[1]}))
                        .on('result', function(res) {
                          res.on('row', function(row) {
                            //console.log('Result row: ' + util.inspect(row));
                          })
                          .on('error', function(err) {
                            console.log('Result error: ' + util.inspect(err));
                          })
                          .on('end', function(info) {
                            // console.log('Result finished successfully');
                            if (info.affectedRows == 1) { // 1 reading was added
                              //TODO: All went well, else report an error
                              //console.log("Inserted:")
                              /*console.log({reading_id : reading_id,
                                            variable_id : current_variable_for_this_channel, 
                                            channel_id : channel_user_given_id_from_topic,
                                            value : packet_payload[1]});*/
                              packet.republish_topic = republish_topic;
                              console.log("Set republish topic to: " + packet.republish_topic);
                              
                              for (var k in self.clients) {
                                var c = self.clients[k]
                                  , republish = false;

                                for (var i = 0; i < c.subscriptions.length; i++) {
                                  var s = c.subscriptions[i];

                                  if (s.test(packet.republish_topic)) {
                                    republish = true;
                                  }
                                }

                                if (republish) {
                                  c.publish({topic: packet.republish_topic, payload: packet.payload});
                                }
                              }
                              
                            }
                          });
                        })
                        .on('end', function() {
                          // console.log('SQL insert done.');
                        });
                      }           
                    });
                  })
                  .on('end', function() {
                    // console.log('SQL insert done.');
                  });
                }
              });
            })
            .on('end', function() {
              // console.log('SQL insert done.');        
            });
          }  
          
	});
      })
      .on('end', function() {
	// console.log('All SQL done.');
        
      });
    } // closing the gauge if test
      
// If we can find the client/username we can get the publishing patterns
//      if no [sensor_Id] then we must add it into the topic
// 	If topic contains a %c, then insert the client id from the client.id
// 	if topic contains a %u, then insert the username from the ???
// 	if topic contains nothing 

// Else we need to find the client id from the topic pattern, in which case it must be first
//      remove the client id from the topic pattern

//     for (var k in self.clients) {
//       var c = self.clients[k]
//         , publish = false;
// 
//       for (var i = 0; i < c.subscriptions.length; i++) {
//         var s = c.subscriptions[i];
// 
//         if (s.test(packet.topic)) {
//           publish = true;
//         }
//       }
// 
//       if (publish) {
//         c.publish({topic: packet.topic, payload: packet.payload});
//       }
//     }
  });

  client.on('pingreq', function(packet) {
    console.log('Ping from client ' + client.id);
    client.pingresp();
  });

  client.on('disconnect', function(packet) {
    client.stream.end();
  });

  client.on('close', function(packet) {
    delete self.clients[client.id];
  });

  client.on('error', function(e) {
    client.stream.end();
    console.log(e);
  });
}).listen(process.argv[2] || 1883);

// Catch a CTRL-C event 
process.on('SIGINT', function () {
  console.log('Got Ctrl+C! Sending all client disconnect packets, and closing the MQTT server.');
  
  // For each client, send a disconnect and close the connection, 
  // probably all synchronous, but who cares when you're closing down?
  for (var clientId in myMQTTServer.clients) {
    console.log('Sending disconnect to client: ' + clientId);
    myMQTTServer.clients[clientId].disconnect();
    myMQTTServer.clients[clientId].stream.end();
  }

  // Now close the server
  myMQTTServer.close();
  
  // Now close the DB connection
  dbclient.end();
});

process.on('uncaughtException',function(error){
  console.log(error);
})
