
var config = {};

// setup the DB connection
config.database = {
      host: 'localhost',
      db: 'your_database_name',
      user: 'your_database_user_and_hopefully_not_root',
      password: 'your_database_password',
    };

exports.getConfiguration = function() {return config;}

