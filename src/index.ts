import { Kafka } from 'kafkajs';
import * as dotenv from 'dotenv';

import { producerMessages, consumerSubscribeConfig, consumerRunConfig } from './types';
import { Message } from 'kafkajs';

dotenv.config();

export class KafeDLQClient {
    kafeAdmin: any;
    kafeProducer: any;
    kafeConsumer: any;
    client: any;
    callback?: (message: any) => boolean;

    constructor(client: any, callback?: any) {

        this.kafeAdmin = client.admin();
        this.kafeProducer = client.producer();
        this.client = client;
        this.callback = callback;
        this.kafeConsumer = null;

        this.sendToDLQ = this.sendToDLQ.bind(this);
        this.createDLQTopic = this.createDLQTopic.bind(this);
    };

    producer(): any {
        const DLQClient = this;
        const { kafeProducer, callback, sendToDLQ } = DLQClient;

        const DLQProducer = {
            ...kafeProducer,
            connect() {
                return kafeProducer.connect();
            },
            async send(producerMessages: producerMessages) {
                const { topic, messages } = producerMessages;
                if (!messages.length) return;

                //Send each message individually to allow for message specific idenificaton of failures
                for (const message of messages) {
                    try {
                        if (callback) {
                            if (callback(message.value) === false) {
                                throw new Error(`Callback failed when producing message: ${message.value}`);
                            };
                        };

                        await kafeProducer.connect();
                        await kafeProducer.send({
                            topic,
                            messages: [message],
                        });

                        await kafeProducer.disconnect();
                    } catch(err: any) {
                        console.log(`Error while sending message ${message.value}: ${err}`);

                        await sendToDLQ(
                            message,
                            topic,
                            'Producer',
                            err.message);
                    };
                };
            },
        };

        return DLQProducer;
    };

    consumer(groupId: { groupId: string }) {

        const DLQClient = this;
        let { kafeConsumer, client, sendToDLQ } = DLQClient;

        kafeConsumer = client.consumer(groupId);
        const DLQConsumer = {
            ...kafeConsumer,
            connect() {
                return kafeConsumer.connect();
            },
            async subscribe(consumerSubscribeConfig?: consumerSubscribeConfig){
                return await kafeConsumer.subscribe({
                    ...consumerSubscribeConfig,
                    fromBeginning: false,
                });
            },
            async run(consumerRunConfig: consumerRunConfig) {
                return kafeConsumer.run({
                    ...consumerRunConfig,
                    eachMessage: async (
                        { topic, partition, message }: { topic: string, partition: number, message: any }
                    ) => {
                        try {
                            await consumerRunConfig.eachMessage({ topic, partition, message })
                        } catch(err) {
                            await sendToDLQ(message, topic, 'Consumer', err);
                        };
                    }
                });
            }
        };

        return DLQConsumer;
    };

    async sendToDLQ(message: Message, originalTopic: string, clientType: string, err: any) {
        if (!message || !originalTopic) return;

        try {
            //Get all existing topics and see if the DLQ topic already exists
            const existingTopics = await this.kafeAdmin.listTopics();
            const DLQTopic = existingTopics.filter((topicName: string) => topicName === 'DeadLetterQueue');

            //If the DLQ topic doesn't exist the create it
            if (!DLQTopic.length) await this.createDLQTopic('DeadLetterQueue');

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

            await this.kafeProducer.connect();
            await this.kafeProducer.send({
                topic: 'DeadLetterQueue',
                messages: [DLQMessage],
            });

            await this.kafeProducer.disconnect();
        } catch(err) {
            console.log('Error while sending message to DLQ: ', err);
        };
    };

    async createDLQTopic(topic: string) {
        try {
            await this.kafeAdmin.connect();
            await this.kafeAdmin.createTopics({
              topics: [topic],
              numPartitions: 1,
              replicationFactor: 1
            });

            await this.kafeAdmin.disconnect();
        } catch(err) {
            console.log(err);
        };
    };
};
