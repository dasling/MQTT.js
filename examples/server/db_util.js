var util = require('util');

exports.log = function(log_msg, log_human_msg, dbclient, debug_to_console) {

  if(typeof(debug_to_console)==='undefined') debug_to_console = 0; // default is no debug 
  
  if(debug_to_console > 1) {  // human output + details
    console.log('***********');
    console.log('Message:' + util.inspect(log_msg));
    console.log('Human message:' + util.inspect(log_human_msg));
  }

  if(debug_to_console == 1) { // only human readable output
    console.log('***********');
    console.log('Human message:' + util.inspect(log_human_msg));
  }

  // except a log message, and log it to the DB
  var pq = dbclient.prepare('INSERT INTO log (message, human_message) VALUES (:message, :human_message)');

  dbclient.query(pq({ message: log_msg, human_message: log_human_msg }))
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
  });
}
