var AWS = require('aws-sdk');
AWS.config.update({ region: 'us-east-1' });

exports.handler = (event, context, callback) => {
    var CWE = new AWS.CloudWatchEvents();

    var params = {
      Name: 'CaptureConstantInterval'
    };

    if (event.toggle === "on") {
        CWE.enableRule(params, function(err, data) {
          if (err) console.log(err, err.stack); // an error occurred
          else console.log("Toggled on");
        });
    } else {
        CWE.disableRule(params, function(err, data) {
          if (err) console.log(err, err.stack); // an error occurred
          else console.log("Toggled off");
        });
    }
};
