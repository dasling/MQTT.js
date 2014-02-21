var mqtt = require('../..')
  , util = require('util')
  , config = require('./config')
  , Dbclient = require('mariasql') // https://github.com/mscdex/node-mariasql
  , db_util = require('./db_util')
  , async = require('async');

var DEBUG_LEVEL = 3    // Set the verbosity for debugging (add 10 to the level to ensure internal details for the level)
  , DEBUG_INFO = 3
  , DEBUG_WARNING = 2
  , DEBUG_ERROR = 1;

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
   console.log('DB Client closed due to: ' + hadError);
 });
 
var myMQTTServer = mqtt.createServer(function(client) {
  // this function is called on a new connection (emiting also "client" event)
  // client argument is a "new Connection (socket, MqttServer)"

  var self = this;

  if (!self.clients) self.clients = {}; 
  
  client.on('connect', function(packet) {

    //console.log(util.inspect(packet.clientId));    
    db_util.log(util.inspect(packet), 'Connect from ' + packet.clientId, dbclient, DEBUG_INFO, DEBUG_LEVEL, packet.clientId);

    // Check the device authorization
    if (packet.username && packet.password && packet.clientId) {
 
      var preproc_sql = dbclient.prepare('SELECT device_id FROM device_auth WHERE client_id = :id AND username = :username AND password = :password AND status_id = 1');
      var sql_stat = preproc_sql({id: packet.clientId, username: packet.username, password: packet.password});
      db_util.log(util.inspect(sql_stat), "SQL - Device authorization check", dbclient, DEBUG_INFO, DEBUG_LEVEL, packet.clientId); 
      dbclient.query(sql_stat)
        .on('result', function(res) {
          client.device_id = [];
	  res.on('row', function(row) { // Device is authorized
            db_util.log(util.inspect(packet), 'Connect authorized from ' + packet.clientId, dbclient, DEBUG_INFO, DEBUG_LEVEL, packet.clientId);
            // Store information inside the client object
            client.authorized = true;
            client.username = packet.username;
            client.password = packet.password;
            client.id = packet.clientId;
            client.device_id.push(row.device_id);
	  })
          .on('error', function(err) {
            db_util.log(sql_stat, 'DB error: ' + util.inspect(err), dbclient, DEBUG_ERROR, DEBUG_LEVEL, packet.clientId);
            console.log('DB error: ' + util.inspect(err) + "(" + util.inspect(sql_stat) + ")");
          })
          .on('end', function(info) {
            if (info.numRows > 1) {
              for (i = 0; i < client.device_id.length; i++) {
	        // This is not necessarily a problem, but only the last device retrieved with these credentials will be stored
        	db_util.log(util.inspect(packet), "Client (" + packet.clientId + ") authorization for multiple devices with same client identification, username and password", dbclient, DEBUG_WARNING, DEBUG_LEVEL, packet.clientId, util.inspect(client.device_id[i]));
              }
            }
            if (info.numRows < 1) {
              // Device is not authorized
              db_util.log(util.inspect(packet), 'Connect with bad authorization (clientId:"' + packet.clientId + '", username: "' + packet.username + '" , password: "' + packet.password + '"). Sending connack with return code 4 (bad username or password)', dbclient, DEBUG_WARNING, DEBUG_LEVEL, packet.clientId);
              client.connack({returnCode: 4});  // Bad username or password
            }
            if (info.numRows >= 1) {
              // TODO: Check for the client id, If a client with the same Client ID is already connected to the server, 
              // the "older" client must be disconnected (e.g. client.stream.end() ) by the server before completing the 
              // CONNECT flow of the new client.
              client.subscriptions = [];      	// Set up the empty subscriptions array for this client
              // Store the client in a associative array
              self.clients[client.id] = client;
              client.connack({returnCode: 0});      
              db_util.log(util.inspect(packet), "Client (" + packet.clientId + ") authorization success", dbclient, DEBUG_WARNING, DEBUG_LEVEL, packet.clientId, util.inspect(client.device_id));
            }
          });
       })
       .on('end', function() {
       });
    }
    else {
      db_util.log(util.inspect(packet), 'CONNECT: Received a connect packet missing a username or a password. Sending connack with return code 4 (bad username or password)', dbclient, DEBUG_WARNING, DEBUG_LEVEL, packet.clientId);
      client.connack({returnCode: 4});  // Bad username or password
    }
  });

  client.on('subscribe', function(packet) {
    // double-check for device authorization
    if (client.authorized != true) {
      return; // MQTT 3.1 says we can't send anything to say that subscribe was denied
    }
    
    var granted = [];

    //  TODO: Need to check whether subscription are allowed based on feature GROUPS to add to the DB still
    for (var i = 0; i < packet.subscriptions.length; i++) {
      var qos = packet.subscriptions[i].qos
        , topic = packet.subscriptions[i].topic
        , reg = new RegExp(topic.replace('+', '[^\/]+').replace('#', '.+$'));

      granted.push(qos);
      client.subscriptions.push(reg);

      // TODO: add device_id & channel_id to the log (need to retrieve the values from the DB first) 
      db_util.log(util.inspect(client.subscriptions), "Client subscribed", dbclient, DEBUG_INFO, DEBUG_LEVEL, client.id, null, topic); // Granted are the granted QoS'es
    }
    client.suback({messageId: packet.messageId, granted: granted}); // Granted are the granted QoS'es
  });

  client.on('publish', function(packet) {
    // double-check for device authorization
    if (client.authorized != true) {
      db_util.log(util.inspect(packet), 'Unauthorized publish', dbclient, DEBUG_WARNING, DEBUG_LEVEL, client.id, null, packet.topic);
      return; // no such thing as sending a negative PUBACK
    }
    else {
      db_util.log(util.inspect(packet), "PUBLISH: client id: "  + client.id + ", payload: " + packet.payload + ", topic: " + packet.topic, dbclient, DEBUG_INFO, DEBUG_LEVEL, client.id, null, packet.topic);
    }
    
    async.waterfall([ // DOC: https://github.com/caolan/async#series
      function(callback) {
        // arg1 will be passed down the waterfall, adding results to it in each function
        var arg1 = {
                    client:  {organization_id : 0,
                              client_id: client.id      // TODO: Need to sanity check this input
                             },            
                    packet:  {topic: packet.topic,
                              payload: packet.payload   // TODO: Need to sanity check this input
                             },
                    channel: {}
               }
        callback(null, arg1);
      }
      , get_channel_and_variable
      , function(arg1, callback) {
          
          // apply the regular expression to the payload 
          var rePattern;
          try {
            // TODO: Need to make a regex which can change the ordering, because now
            // timestamp needs to be first subgroup, value second subgroup
            // E.g.: /[(\[1-9\]*),(.*), (.*)\]/gm;
            //console.log('regex:' + util.inspect(arg1.channel.payload_regexp));
            if (arg1.channel.payload_regexp == '' || arg1.channel.payload_regexp == null) {
              arg1.channel.payload_regexp = "/\\[([0-9]*),([0-9]*),(.*)\\]/gm"; // default regexp
            }
            var flags = arg1.channel.payload_regexp.replace(/.*\/([gimy]*)$/, '$1');
            var pattern = arg1.channel.payload_regexp.replace(new RegExp('^/(.*?)/'+flags+'$'), '$1');
            var rePattern = new RegExp(pattern, flags);
            
            var matches = rePattern.exec(arg1.packet.payload);
            arg1.packet.timestamp = matches[1];
            arg1.packet.value = matches[2];
            //console.log(arg1.packet.payload);
            //console.log(arg1.packet.value);
	    callback(null, arg1);
          } 
          catch (err) {
            db_util.log(err, err, dbclient, DEBUG_WARNING, DEBUG_LEVEL, arg1.client.client_id, arg1.device.device_manufacturer_identification, packet.topic, arg1.variable.name);
            callback("Regular expression failed to execute on payload");
          }
        }
      , insert_reading_and_value
      , function(arg1, callback) { // republishing
          if (arg1.variable.republish_topic == null || arg1.variable.republish_topic == '') { 
            db_util.log(arg1, "No republishing set", dbclient, DEBUG_INFO, DEBUG_LEVEL, arg1.client.client_id, arg1.device.device_manufacturer_identification, packet.topic, arg1.variable.name);
          }
          else {
            db_util.log(arg1, "Republishing if subscriptions to " + arg1.variable.republish_topic, dbclient, DEBUG_INFO, DEBUG_LEVEL, arg1.client.client_id, arg1.device.device_manufacturer_identification, packet.topic, arg1.variable.name);
          }

          for (var k in self.clients) {
            var c = self.clients[k]
              , republish = false;

            for (var i = 0; i < c.subscriptions.length; i++) {
              var s = c.subscriptions[i];

              if (s.test(arg1.variable.republish_topic)) { // only republish if clients are subscribed to it
                republish = true;
              }
            }

            if (republish) {
              var republish_mqtt_msg = {topic: arg1.variable.republish_topic, payload: arg1.packet.payload};
              c.publish(republish_mqtt_msg);
              db_util.log(republish_mqtt_msg,
                          "Republished: " + util.inspect(republish_mqtt_msg), dbclient, DEBUG_INFO, DEBUG_LEVEL, arg1.client.client_id, arg1.device.device_manufacturer_identification, packet.topic, arg1.variable.name);
            }
          }
        }
    ],
    // optional callback
    function(err, results){
      // TODO: Need to log succes or error IN DB
    }); 
  });
  
  client.on('pingreq', function(packet) {
    db_util.log(util.inspect(packet), 'Ping request.', dbclient, DEBUG_INFO, DEBUG_LEVEL, client.id);
    client.pingresp();
  });

  client.on('disconnect', function(packet) {
    db_util.log(util.inspect(packet), 'Client disconnected.', dbclient, DEBUG_INFO, DEBUG_LEVEL, client.id);
    client.stream.end();
  });

  client.on('close', function(packet) {
    db_util.log(util.inspect(packet), 'Client closed connection.', dbclient, DEBUG_INFO, DEBUG_LEVEL, client.id);
    delete self.clients[client.id];
  });

  client.on('error', function(e) {
    db_util.log(e, 'MQTT client error, closing stream.', dbclient, DEBUG_ERROR, DEBUG_LEVEL, client.id);
    client.stream.end();
    console.log(e);
  });
}).listen(process.argv[2] || 1883);

// Catch a CTRL-C event 
process.on('SIGINT', function () {
  // Not an error, but using the error level for this output anyway
  db_util.log(null, 'Got Ctrl+C! Sending all client disconnect packets, and closing the MQTT server', dbclient, DEBUG_ERROR, DEBUG_LEVEL);
  
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
  
  // exit the process
   process.exit(0);
})

process.on('uncaughtException',function(error){
  db_util.log(error, 'uncaughtException in MQTT server', dbclient, DEBUG_ERROR, DEBUG_LEVEL);
  console.log(error);
  process.exit(1);
})

var get_channel_and_variable = function (arg1, callback) {
  
  var preproc_sql = dbclient.prepare('SELECT a.client_id AS client_id, c.channel_id AS channel_id, c.payload_regexp as payload_regexp \
                                             , v.variable_id, v.republish_topic, v.store_in_DB, v.name \
                                             , d.device_manufacturer_identification \
                                      FROM channels c \
                                        JOIN devices d \
                                          ON c.device_id = d.device_id AND c.organization_id = d.organization_id \
                                        JOIN device_auth a \
                                          on d.device_id = a.device_id AND d.organization_id = a.organization_id \
                                        JOIN variable v \
                                      WHERE c.channel_user_given_id = :channel_user_given_id \
                                        AND c.organization_id = :organization_id \
                                        AND v.organization_id = :organization_id \
                                        AND v.current_channel_id = c.channel_id \
                                        AND a.client_id = :client_id \
                                      LIMIT 1');
  var sql_stat = preproc_sql({channel_user_given_id: arg1.packet.topic, 
                              organization_id: arg1.client.organization_id,
                              client_id: arg1.client.client_id});
  dbclient.query(sql_stat)
  .on('result', function(res) {
    res.on('row', function(row) {
      arg1.channel = {client_id: row.client_id, channel_id: row.channel_id, payload_regexp: row.payload_regexp};
      arg1.variable = {variable_id: row.variable_id, republish_topic: row.republish_topic, store_in_DB: row.store_in_DB, name:row.name};
      arg1.device = {device_manufacturer_identification: row.device_manufacturer_identification};
    })
    .on('error', function(err) {
      db_util.log(sql_stat, 'DB error: ' + util.inspect(err), dbclient, DEBUG_ERROR, DEBUG_LEVEL, arg1.client.client_id, null, arg1.packet.topic);
      console.log('DB error: ' + util.inspect(err) + "(" + util.inspect(sql_stat) + ")");
    })
    .on('end', function(info) {
      if (info.numRows != 1) { // No authorization
        var error_statement = "Client_id " + arg1.client.client_id + " not authorized for channel " + arg1.packet.topic;
        db_util.log(util.inspect(sql_stat), error_statement, dbclient, DEBUG_WARNING, DEBUG_LEVEL, arg1.client.client_id, null, arg1.packet.topic);
        callback(error_statement);
      } else { // All is fine
        callback(null, arg1);
      }    
    });
  })
  .on('end', function() {
  });
}

var insert_reading_and_value = function (arg1, callback) {
  
  if (arg1.variable.store_in_DB == 1 || arg1.variable.store_in_DB == '1') {
    db_util.log(arg1.variable, 'Storing in DB', dbclient, DEBUG_INFO, DEBUG_LEVEL, arg1.client.client_id, arg1.device.device_manufacturer_identification, arg1.packet.topic, arg1.variable.name);

    var preproc_sql = dbclient.prepare(
      'INSERT INTO readings (organization_id, measured_at_timestamp) ' 
      + 'VALUES (:organization_id, :measured_at_timestamp)');
    var sql_stat = preproc_sql({organization_id : arg1.client.organization_id, measured_at_timestamp : arg1.packet.timestamp}); 
    
    dbclient.query(sql_stat)
    .on('result', function(res) {
      res.on('row', function(row) {
      })
      .on('error', function(err) {
        db_util.log(sql_stat, 'DB error: ' + util.inspect(err), dbclient, DEBUG_ERROR, DEBUG_LEVEL, arg1.client.client_id, arg1.device.device_manufacturer_identification, arg1.packet.topic, arg1.variable.name);
        console.log('DB error: ' + util.inspect(err) + "(" + util.inspect(sql_stat) + ")");
      })
      .on('end', function(info) {
        if (info.affectedRows == 1) { // 1 reading was added
          var reading_id = info.insertId;
                
          // Now add the values for this reading
          var preproc_sql = dbclient.prepare(
              'INSERT INTO value (organization_id, reading_id, variable_id, channel_id, value) ' 
              + 'VALUES (:organization_id, :reading_id, :variable_id, :channel_id, :value)');
          var sql_stat = preproc_sql({organization_id : arg1.client.organization_id, 
                                      reading_id : reading_id,
                                      variable_id : arg1.variable.variable_id, 
                                      channel_id : arg1.channel.channel_id,
                                      value : arg1.packet.value});
          
          dbclient.query(sql_stat)
          .on('result', function(res) {
            res.on('row', function(row) {
            })
            .on('error', function(err) {
              db_util.log(sql_stat, 'DB error: ' + util.inspect(err), dbclient, DEBUG_ERROR, DEBUG_LEVEL, arg1.client.client_id, arg1.device.device_manufacturer_identification, arg1.packet.topic, arg1.variable.name);
              console.log('DB error: ' + util.inspect(err) + "(" + util.inspect(sql_stat) + ")");
              callback('Error inserting value in DB' + util.inspect(err)); 
            })
            .on('end', function(info) {
              if (info.affectedRows == 1) { // 1 value was added
                callback(null, arg1);
              }
            });
          })
          .on('end', function() {
          });
        }           
      });
    })
    .on('end', function() {
    });
  }
  else {
    db_util.log(arg1.variable, 'Not storing in DB', dbclient, DEBUG_INFO, DEBUG_LEVEL, arg1.client.client_id, arg1.device.device_manufacturer_identification, arg1.packet.topic, arg1.variable.name);
    callback(null, arg1);
  }
}
