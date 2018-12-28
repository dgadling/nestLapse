var rp = require('request-promise');

var AWS = require('aws-sdk');
AWS.config.update({ region: 'us-east-1' });

function capture_and_store(kind, when, continuous) {
    console.log(`trying to capture ${kind}`);
    return rp({"url": process.env[`${kind}_url`], "encoding": null}
    ).then(function(body) {
        console.log(`captured ${kind}, persisting to S3`);
        var s3 = new AWS.S3();

        var path = `${kind}/${when}.jpg`;
        if (continuous === true) {
            path = `continuous/${path}`;
        }
        var params = {
            Bucket : 'nestlapse',
            Key : path,
            Body : body
        };

        s3.putObject(params, function(err, data) {
          if (err) console.log(kind, when, err, err.stack); // an error occurred
        });
    });
}

exports.handler = (event, context, callback) => {
    if (event.continuous === undefined) {
        event.continuous = false;
    }
    var now = (new Date().getTime() / 1000).toFixed();
    return Promise.all([
        capture_and_store('backdoor', now, event.continuous),
        capture_and_store('backpatio', now, event.continuous),
    ]);
};