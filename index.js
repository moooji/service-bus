"use strict";

var Promise = require("bluebird");
var AWS = require('aws-sdk');

function serviceBus(options) {

    validateOptions(options);

    let _isPolling = false;
    let _subDelegate;

    const _pubQueueUrl = options.pubQueueUrl;
    const _subQueueUrl = options.subQueueUrl;

    const _sqs = new AWS.SQS({
        accessKeyId: options.accessKeyId,
        secretAccessKey: options.secretAccessKey,
        region: options.region
    });

    const receiveMessagesAsync = Promise.promisify(_sqs.receiveMessage, _sqs);
    const deleteMessageAsync = Promise.promisify(_sqs.deleteMessage, _sqs);
    const sendMessageAsync = Promise.promisify(_sqs.sendMessage, _sqs);

    function acknowledge(message, callback) {

        return deleteMessageAsync({
            QueueUrl: _subQueueUrl,
            ReceiptHandle: message.ReceiptHandle
        }).nodeify(callback);
    }

    function publish(body, callback) {

        return sendMessageAsync({
            QueueUrl: _pubQueueUrl,
            MessageBody: JSON.stringify(body),
            DelaySeconds: 0
        }).nodeify(callback);
    }

    function subscribe(subDelegate, callback) {

        return new Promise(function(resolve, reject) {

            if (!subDelegate || typeof subDelegate !== "function") {
                return reject(new Error("No subDelegate function provided"));
            }

            _subDelegate = subDelegate;
            next();

            return resolve();

        }).nodeify(callback);
    }

    function poll() {

        _isPolling = true;

        const params = {
            QueueUrl: _subQueueUrl,
            MaxNumberOfMessages: 10,
            VisibilityTimeout: 60,
            WaitTimeSeconds: 20
        };

        receiveMessagesAsync(params)
            .then(function(data) {

                _isPolling = false;

                if (data.Messages) {
                    _subDelegate(data.Messages, next);
                }
                else {
                    next();
                }
            })
            .catch(function(err) {

                console.log(err);
                console.log("Retry in 10");

                _isPolling = false;
                setTimeout(next, 10000);
            });
    }

    function next() {

        if(_isPolling) return;
        poll();
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

        if(!options.pubQueueUrl) {
            throw Error("No AWS SQS 'pubQueueUrl' provided");
        }

        if(!options.subQueueUrl) {
            throw Error("No AWS SQS 'subQueueUrl' provided");
        }
    }

    return {
        publish: publish,
        subscribe: subscribe,
        acknowledge: acknowledge
    };
}

module.exports = serviceBus;