var util = require('util');

exports.log = function(log_msg, log_human_msg, dbclient) {
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
