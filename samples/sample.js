var ServiceBus = require("../main");

var sb = ServiceBus({
    accessKeyId: process.env["AWS_ACCESS_KEY_ID"],
    secretAccessKey: process.env["AWS_SECRET_ACCESS_KEY"],
    region: process.env["AWS_SQS_REGION"],
    pubQueueUrl: process.env["AWS_SQS_PUB_QUEUE_URL"],
    subQueueUrl: process.env["AWS_SQS_SUB_QUEUE_URL"]
});

sb.subscribe(onMessages, function(err, res){
    if(err) throw err;
    console.log("Subscribed");
});

function onMessages(messages, done) {

    messages.forEach(function(message) {

        console.log(message);

        sb.acknowledge(message)
            .then(function() {
                console.log("Acknowledged");
            });
    });

    done();
}