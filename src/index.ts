import { Kafka } from 'kafkajs';
import * as dotenv from 'dotenv';

import { producerMessages, consumerSubscribeConfig, consumerRunConfig } from './types';
import { Message, KafkaJSProtocolError } from 'kafkajs';

dotenv.config();

class KafeDLQClient {
    kafeAdmin: any;
    kafeProducer: any;
    kafeConsumer: any;
    client: any;
    callback?: (message: any) => boolean; 

    constructor(client: any, callback?: Function) {

        this.kafeAdmin = client.admin();
        this.kafeProducer = client.producer();
        this.client = client;
        this.kafeConsumer = null;
    };

    producer(): any {
        const DLQClient = this;
        const { kafeProducer } = DLQClient;

        console.log('Original Kafka producer: ', kafeProducer);

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
                        await kafeProducer.connect();
                        await kafeProducer.send({
                            topic,
                            messages: [message],
                        });

                        await kafeProducer.disconnect();
                    } catch(err: any) {
                        console.log(`Error while sending message ${message}: ${err}`);

                        await this.sendToDLQ(
                            message,
                            topic, 
                            'Producer',
                            err);
                    };
                };  
            },
        };

        return DLQProducer;
    };

    consumer(groupId: { groupId: string }) {
        this.kafeConsumer = this.client.consumer(groupId);
        const DLQConsumer = {
            ...this.kafeConsumer,
            connect() {
                return this.kafeConsumer.connect();
            },
            async subscribe(consumerSubscribeConfig?: consumerSubscribeConfig){
                await this.kafeConsumer.subscribe({
                    ...consumerSubscribeConfig,
                    fromBeginning: true,
                });
            },
            async run(consumerRunConfig: consumerRunConfig) {
                return this.kafeConsumer.run({
                    ...consumerRunConfig,
                    eachMessage: async (
                        { topic, partition, message }: { topic: string, partition: number, message: any }
                    ) => {
                        try {
                            await consumerRunConfig.eachMessage({ topic, partition, message })
                        } catch(err) {
                            await this.sendToDLQ(message, topic, 'Consumer', err);
                        };
                    }
                });
            }
        };

        return DLQConsumer;
    };

    async sendToDLQ(message: Message, originalTopic: string, clientType: string, error: any) {
        if (!message || !originalTopic) return;

        const DLQClient = this;
        const { kafeAdmin, kafeProducer } = DLQClient;

        try {
            //Get all existing topics and see if the DLQ topic already exists
            const existingTopics = await kafeAdmin.listTopics();
            const DLQTopic = existingTopics.filter((topicName: string) => topicName === 'DeadLetterQueue');

            //If the DLQ topic doesn't exist the create it
            if (!DLQTopic.length) await this.createDLQTopic('DeadLetterQueue');

            const DLQMessage = {
                timestamp: new Date().toLocaleString('en-US', {
                    timeStyle: 'long',
                    dateStyle: 'short',
                    hour12: false,
                }),
                value: {
                    originalMessage: message.value,
                    originalTopic: originalTopic,
                    error,
                    clientType,
                },
            };

            await kafeProducer.connect();
            await kafeProducer.send({
                topic: 'DeadLetterQueue',
                messages: [DLQMessage],
            });

            await kafeProducer.disconnect();
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

const kafka = new Kafka({
    clientId: 'test-client',
    brokers: ['localhost:9091', 'localhost:9092', 'localhost:9093']
});

const testClient = new KafeDLQClient(kafka);
testClient.producer();
