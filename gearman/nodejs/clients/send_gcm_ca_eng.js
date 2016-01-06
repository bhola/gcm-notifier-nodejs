var path ='/usr/local/lib//node_modules/';
var Gearman = require(path+"gearmanode");
var client = Gearman.client();
var message_data={
        'notification_id':'-2',
        'notification_language_id':'1',
        'notification_title':'Daily questions',
        'notification_type':'10',
        'notification_description':'New current affairs questions available.'
       };
var message_data_json=JSON.stringify(message_data);
var job = client.submitJob('gcm_notification_nodejs', message_data_json,{background: true});
job.on('workData', function(data) {
    console.log('WORK_DATA >>> ' + data);
});
job.on('complete', function() {
    console.log('RESULT >>> ' + job.response);
    client.close();
});

