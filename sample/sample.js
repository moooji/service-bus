var ServiceBus = require("service-bus");

var sb = ServiceBus({
    accessKeyId: "...",
    secretAccessKey: "...",
    region: "eu-central-1",
    pubQueueUrl: "https://sqs.eu-central-1.amazonaws.com/...",
    subQueueUrl: "https://sqs.eu-central-1.amazonaws.com/..."
});

sb.subscribe(onMessages, function(err, res){
    if(err) throw err;
    console.log("Subscribed");
});

function onMessages(messages, done) {

    messages.forEach(function(message) {

        console.log(message.MessageId);

        sb.acknowledge(message)
            .then(function() {
                console.log("Acknowledged");
            });
    });

    done();
}