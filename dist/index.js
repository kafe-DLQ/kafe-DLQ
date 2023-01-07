"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.KafeDLQClient = void 0;
const dotenv = __importStar(require("dotenv"));
dotenv.config();
class KafeDLQClient {
    constructor(client, callback) {
        this.kafeAdmin = client.admin();
        this.kafeProducer = client.producer();
        this.client = client;
        this.callback = callback;
        this.kafeConsumer = null;
        this.sendToDLQ = this.sendToDLQ.bind(this);
        this.createDLQTopic = this.createDLQTopic.bind(this);
    }
    ;
    producer() {
        const DLQClient = this;
        const { kafeProducer, callback, sendToDLQ } = DLQClient;
        const DLQProducer = Object.assign(Object.assign({}, kafeProducer), { connect() {
                return kafeProducer.connect();
            },
            send(producerMessages) {
                return __awaiter(this, void 0, void 0, function* () {
                    const { topic, messages } = producerMessages;
                    if (!messages.length)
                        return;
                    //Send each message individually to allow for message specific idenificaton of failures
                    for (const message of messages) {
                        try {
                            if (callback) {
                                if (callback(message.value) === false) {
                                    throw new Error(`Callback failed when producing message: ${message.value}`);
                                }
                                ;
                            }
                            ;
                            yield kafeProducer.connect();
                            yield kafeProducer.send({
                                topic,
                                messages: [message],
                            });
                            yield kafeProducer.disconnect();
                        }
                        catch (err) {
                            console.log(`Error while sending message ${message.value}: ${err}`);
                            yield sendToDLQ(message, topic, 'Producer', err.message);
                        }
                        ;
                    }
                    ;
                });
            } });
        return DLQProducer;
    }
    ;
    consumer(groupId) {
        const DLQClient = this;
        let { kafeConsumer, client, sendToDLQ } = DLQClient;
        kafeConsumer = client.consumer(groupId);
        const DLQConsumer = Object.assign(Object.assign({}, kafeConsumer), { connect() {
                return kafeConsumer.connect();
            },
            subscribe(consumerSubscribeConfig) {
                return __awaiter(this, void 0, void 0, function* () {
                    return yield kafeConsumer.subscribe(Object.assign(Object.assign({}, consumerSubscribeConfig), { fromBeginning: false }));
                });
            },
            run(consumerRunConfig) {
                return __awaiter(this, void 0, void 0, function* () {
                    return kafeConsumer.run(Object.assign(Object.assign({}, consumerRunConfig), { eachMessage: ({ topic, partition, message }) => __awaiter(this, void 0, void 0, function* () {
                            try {
                                yield consumerRunConfig.eachMessage({ topic, partition, message });
                            }
                            catch (err) {
                                yield sendToDLQ(message, topic, 'Consumer', err);
                            }
                            ;
                        }) }));
                });
            } });
        return DLQConsumer;
    }
    ;
    sendToDLQ(message, originalTopic, clientType, err) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!message || !originalTopic)
                return;
            try {
                //Get all existing topics and see if the DLQ topic already exists
                const existingTopics = yield this.kafeAdmin.listTopics();
                const DLQTopic = existingTopics.filter((topicName) => topicName === 'DeadLetterQueue');
                //If the DLQ topic doesn't exist the create it
                if (!DLQTopic.length)
                    yield this.createDLQTopic('DeadLetterQueue');
                const dlqMessageValue = {
                    originalMessage: message.value,
                    originalTopic: originalTopic,
                    clientType,
                    err,
                };
                const DLQMessage = {
                    timestamp: Date.now(),
                    value: JSON.stringify(dlqMessageValue),
                };
                yield this.kafeProducer.connect();
                yield this.kafeProducer.send({
                    topic: 'DeadLetterQueue',
                    messages: [DLQMessage],
                });
                yield this.kafeProducer.disconnect();
            }
            catch (err) {
                console.log('Error while sending message to DLQ: ', err);
            }
            ;
        });
    }
    ;
    createDLQTopic(topic) {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                yield this.kafeAdmin.connect();
                yield this.kafeAdmin.createTopics({
                    topics: [topic],
                    numPartitions: 1,
                    replicationFactor: 1
                });
                yield this.kafeAdmin.disconnect();
            }
            catch (err) {
                console.log(err);
            }
            ;
        });
    }
    ;
}
exports.KafeDLQClient = KafeDLQClient;
;
