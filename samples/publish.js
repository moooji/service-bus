var ServiceBus = require("../main");

var sb = ServiceBus({
    accessKeyId: process.env["AWS_ACCESS_KEY_ID"],
    secretAccessKey: process.env["AWS_SECRET_ACCESS_KEY"],
    region: process.env["AWS_SQS_REGION"],
    pubQueueUrl: process.env["AWS_SQS_PUB_QUEUE_URL"],
    subQueueUrl: process.env["AWS_SQS_SUB_QUEUE_URL"]
});

sb.publish({
    name: "Peter",
    phone: 1234
}, function(err, res) {

    if(err) {
        console.log(err);
    }

    console.log(res);
});