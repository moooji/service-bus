"use strict";

var crypto = require("crypto");
var zlib = require("zlib");
var Promise = require("bluebird");
var AWS = require("aws-sdk");
var createError = require("custom-error-generator");
var _ = require("lodash");

var MessageError = createError('MessageError');
var InvalidArgumentError = createError('InvalidArgumentError');

function serviceBus(options) {

    validateOptions(options);

    var _isPolling = false;
    var _subDelegate;

    var _pubQueueUrl = options.pubQueueUrl;
    var _subQueueUrl = options.subQueueUrl;

    var _sqs = new AWS.SQS({
        accessKeyId: options.accessKeyId,
        secretAccessKey: options.secretAccessKey,
        region: options.region
    });

    var receiveMessagesAsync = Promise.promisify(_sqs.receiveMessage, _sqs);
    var deleteMessageAsync = Promise.promisify(_sqs.deleteMessage, _sqs);
    var sendMessageAsync = Promise.promisify(_sqs.sendMessage, _sqs);

    function acknowledge(message, callback) {

        return deleteMessageAsync({
            QueueUrl: _subQueueUrl,
            ReceiptHandle: message.receiptHandle
        }).nodeify(callback);
    }

    function publish(data, callback) {

        return zipData(data)
            .then(function (zippedData) {

                return sendMessageAsync({
                    QueueUrl: _pubQueueUrl,
                    MessageBody: "gzip",
                    DelaySeconds: 0,
                    MessageAttributes: {
                        data: {
                            DataType: "Binary",
                            BinaryValue: zippedData
                        }
                    }
                });
            })
            .then(function (message) {

                // TODO: ADD MD5 check
                return message;
            })
            .nodeify(callback);
    }

    function subscribe(subDelegate, callback) {

        return new Promise(function (resolve, reject) {

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

        var params = {
            QueueUrl: _subQueueUrl,
            MaxNumberOfMessages: 10,
            VisibilityTimeout: 60,
            WaitTimeSeconds: 10,
            MessageAttributeNames: ["data"]
        };

        receiveMessagesAsync(params)
            .then(function (data) {

                _isPolling = false;

                if (data && data.Messages) {
                    //var messages = parseMessages(data.Messages);
                    //_subDelegate(messages, next);

                    return parseMessages(data.Messages)
                        .then(function(messages) {
                           _subDelegate(messages, next);
                        });
                }
                else {
                    next();
                }
            })
            .catch(function (err) {

                _isPolling = false;
                throw err;
            });
    }

    function next() {

        console.log("next %s", _isPolling);
        if (_isPolling) return;
        poll();
    }

    function parseMessages(messages, callback) {

        return Promise.resolve(messages)
            .map(function(message) {

                return Promise.resolve(message)
                    .then(function(message) {

                        if (!message.MessageAttributes ||
                            !message.MessageAttributes.data ||
                            !message.MessageAttributes.data.BinaryValue) {

                            throw MessageError("Message has invalid payload");
                        }

                        return unzipData(message.MessageAttributes.data.BinaryValue);
                    })
                    .then(function(body) {

                        return {
                            messageId: message.MessageId,
                            receiptHandle: message.ReceiptHandle,
                            body: body
                        }
                    });

            }).nodeify(callback);
    }

    function zipData(data, callback) {

        return new Promise(function (resolve, reject) {

            try {
                var dataString = JSON.stringify(data);
                var dataBuffer = new Buffer(dataString);

                zlib.gzip(dataBuffer, function (err, res) {

                    if (err) {
                        return reject(err);
                    }

                    return resolve(res);
                });
            }
            catch (err) {
                return reject(err);
            }
        }).nodeify(callback);
    }

    function unzipData(zippedData, callback) {

        return new Promise(function (resolve, reject) {

            zlib.gunzip(zippedData, function (err, res) {

                if (err) {
                    return reject(err);
                }

                try {
                    var data = JSON.parse(res);
                    return resolve(data);
                }
                catch (err) {
                    return reject(err);
                }
            });
        }).nodeify(callback);
    }

    function validateOptions(options) {

        if (!options.accessKeyId) {
            throw InvalidArgumentError("No AWS 'accessKeyId' provided");
        }

        if (!options.secretAccessKey) {
            throw InvalidArgumentError("No AWS 'secretAccessKey' provided");
        }

        if (!options.region) {
            throw InvalidArgumentError("No AWS 'region' provided");
        }

        if (!options.pubQueueUrl) {
            throw InvalidArgumentError("No AWS SQS 'pubQueueUrl' provided");
        }

        if (!options.subQueueUrl) {
            throw InvalidArgumentError("No AWS SQS 'subQueueUrl' provided");
        }
    }

    return {
        publish: publish,
        subscribe: subscribe,
        acknowledge: acknowledge,
        InvalidArgumentError: InvalidArgumentError,
        MessageError: MessageError
    };
}

module.exports = serviceBus;