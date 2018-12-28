var rp = require('request-promise');

var AWS = require('aws-sdk');
AWS.config.update({ region: 'us-east-1' });

function date_to_cron(d) {
    var i = {
        'min': d.getUTCMinutes(),
        'hr': d.getUTCHours(),
        'date': d.getUTCDate(),
        'mon': d.getUTCMonth() + 1,
        'yr': d.getUTCFullYear(),
    };

    // minutes hours DoM mon DoW year
    // NOTE: Only DoM or DoW can be specified, put a ? for the other
    return `${i['min']} ${i['hr']} ${i['date']} ${i['mon']} ? ${i['yr']}`;
}

function schedule(name, time) {
    var CWE = new AWS.CloudWatchEvents();

    var params = {
        Name: name,
        Description: `${name} @ ${time}`,
        ScheduleExpression: `cron(${date_to_cron(time)})`,
        State: "ENABLED",
    };

    console.log(`${name} = ${time} -> ${params.ScheduleExpression}`);
    return CWE.putRule(params).promise();
}

function generate_times(info) {
    var times = {
        'twilight_start': new Date(info['civil_twilight_begin']),
        'sunrise': new Date(info['sunrise']),
        'mid_morning': new Date(),  // we'll fill this in shortly
        'noon': new Date(info['solar_noon']),
        'mid_afternoon': new Date(),  // we'll fill this in shortly
        'sunset': new Date(info['sunset']),
        'twilight_end': new Date(info['civil_twilight_end']),
    };

    var morning_delta = (times.noon.getTime() - times.sunrise.getTime()) / 2;
    var afternoon_delta = (times.sunset.getTime() - times.noon.getTime()) / 2;
    times.mid_morning.setTime(times.sunrise.getTime() + morning_delta);
    times.mid_afternoon.setTime(times.noon.getTime() + afternoon_delta);

    console.log(times);

    return times;
}

exports.handler = (event, context, callback) => {
    var lat = process.env['lat'] || 37.560227;
    var lng = process.env['lng'] || -122.3251753;

    /*
     Time Info (what's civil twilight?)
     https://www.timeanddate.com/astronomy/different-types-twilight.html
     */
    var url = `https://api.sunrise-sunset.org/json?lat=${lat}&lng=${lng}&formatted=0`;

    console.log("Fetching info")
    return rp(url).then(function(body) {
        console.log("Got info, generating schedule");
        var times = generate_times(JSON.parse(body)['results']);

        return Promise.all([
            schedule('1_nestCaptureTwilightBegin', times.twilight_start),
            schedule('2_nestCaptureSunrise', times.sunrise),
            schedule('StartConstantCapture', times.sunrise),
            schedule('3_nestCaptureMidMorning', times.mid_morning),
            schedule('4_nestCaptureSolarNoon', times.noon),
            schedule('5_nestCaptureMidAfternoon', times.mid_afternoon),
            schedule('6_nestCaptureSunset', times.sunset),
            schedule('StopConstantCapture', times.sunset),
            schedule('7_nestCaptureTwilightEnd', times.twilight_end),
        ]);
    });
};