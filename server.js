var Promise = require("bluebird");
var AWS = require('aws-sdk');

function serviceBus(onMessages, options) {

    validateOptions(options);

    var isPolling = false;
    var requestQueueUrl = options.requestQueueUrl;
    var responseQueueUrl = options.responseQueueUrl;

    var sqs = new AWS.SQS({
        accessKeyId: options.accessKeyId,
        secretAccessKey: options.secretAccessKey,
        region: options.region
    });

    var receiveMessagesAsync = Promise.promisify(sqs.receiveMessage, sqs);
    var deleteMessageAsync = Promise.promisify(sqs.deleteMessage, sqs);
    var sendMessageAsync = Promise.promisify(sqs.sendMessage, sqs);

    function acknowledgeMessage(message, callback) {

        return deleteMessageAsync({
            QueueUrl: responseQueueUrl,
            ReceiptHandle: message.ReceiptHandle
        }).nodeify(callback);
    }

    function sendMessage(body, callback) {
        
        return sendMessageAsync({
            QueueUrl: requestQueueUrl,
            MessageBody: JSON.stringify(body),
            DelaySeconds: 0
        }).nodeify(callback);
    }

    function pollMessages() {

        isPolling = true;

        var options = {
            QueueUrl: responseQueueUrl,
            MaxNumberOfMessages: 10,
            VisibilityTimeout: 60,
            WaitTimeSeconds: 20
        };

        receiveMessagesAsync(options)
            .then(function(data) {

                isPolling = false;

                if (data.Messages) {
                    onMessages(data.Messages, function() {
                        next();
                    });
                }
                else {
                    next();
                }
            })
            .catch(function(err) {

                console.log(err);
                console.log("Retry in 10");

                isPolling = false;
                setTimeout(next, 10000);
            });
    }

    function next() {

        if(isPolling) return;
        pollMessages();
    }

    function validateOptions(options) {

        if(!options.accessKeyId) {
            throw Error("No AWS 'accessKeyId' provided");
        }

        if(!options.secretAccessKey) {
            throw Error("No AWS 'secretAccessKey' provided");
        }

        if(!options.region) {
            throw Error("No AWS 'region' provided");
        }

        if(!options.requestQueueUrl) {
            throw Error("No AWS SQS 'requestQueueUrl' provided");
        }

        if(!options.responseQueueUrl) {
            throw Error("No AWS SQS 'responseQueueUrl' provided");
        }
    }

    return {
        send: sendMessage,
        acknowledge: acknowledgeMessage
    };
}

module.exports = serviceBus;