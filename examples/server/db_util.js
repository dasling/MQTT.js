var util = require('util');

exports.log = function(log_msg, log_human_msg, dbclient, debug_type, debug_level, client, device, channel, variable) {

  if(typeof(debug_level)==='undefined') debug_level = 0; // default is no debug 
  if(typeof(client)==='undefined') client = 'N/A'; 
  if(typeof(device)==='undefined') device = 'N/A';
  if(typeof(channel)==='undefined') channel = 'N/A';
  if(typeof(variable)==='undefined') variable = 'N/A';
  
  // No output of machine format below debug_level 10
  if((debug_level >= debug_type) && (debug_level < 10)) {  // human output 
    console.log('***********');
    console.log('Human message:' + util.inspect(log_human_msg));
  }
  
  // Output also machine format if debug_level > 10
  if(debug_level >= (debug_type + 10)) {  // human output 
    console.log('***********');
    console.log('Message:' + util.inspect(log_msg));
    console.log('Human message:' + util.inspect(log_human_msg));
  }

  // except a log message, and log it to the DB
  var pq = dbclient.prepare('INSERT INTO log (message, human_message, client, device, channel, variable) VALUES (:message, :human_message, :client, :device, :channel, : variable)');

  dbclient.query(pq({message: log_msg, human_message: log_human_msg, client: client, device: device, channel: channel, variable: variable }))
  .on('result', function(res) {
    res.on('row', function(row) {
    })
    .on('error', function(err) {
      console.log('Error logging to DB log: ' + util.inspect(err));
      console.log('Message:' + util.inspect(log_msg));
      console.log('Human message:' + util.inspect(log_human_msg));
    })
    .on('end', function(info) {
    });
  })
  .on('end', function() {
  });}
