import { Kafka } from 'kafkajs';
import * as dotenv from 'dotenv';

import { producerMessages } from './types';
import { Message, KafkaJSProtocolError } from 'kafkajs';

dotenv.config();

class KafeDLQClient {
    kafeAdmin: any;
    kafeProducer: any;
    kafeConsumer: any;
    customDLQ: boolean;
    callback?: (message: any) => boolean; 

    constructor(client: any, customDLQ: boolean = false, callback?: Function) {

        this.kafeAdmin = client.admin();
        this.kafeProducer = client.producer();
        this.customDLQ = customDLQ;
    };

    producer(): any {
        const DLQClient = this;
        const { kafeProducer, customDLQ } = DLQClient;

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
                            customDLQ ? `DLQ-${topic}` : 'DeadLetterQueue', 
                            message, 
                            'Producer',
                            err);
                    };
                };  
            },
        };

        return DLQProducer;
    };

    async sendToDLQ(topic: string, message: Message, clientType: string, error: any) {
        if (!topic) return;

        const DLQClient = this;
        const { kafeAdmin, kafeProducer } = DLQClient;

        try {
            //Get all existing topics and see if the DLQ topic already exists
            const existingTopics = await kafeAdmin.listTopics();
            const DLQTopic = existingTopics.filter((topicName: string) => topicName === topic);

            //If the DLQ topic doesn't exist the create it
            if (!DLQTopic.length) await this.createDLQTopic(topic);

            const DLQMessage = {
                timestamp: new Date().toLocaleString('en-US', {
                    timeStyle: 'long',
                    dateStyle: 'short',
                    hour12: false,
                }),
                value: {
                    originalMessage: message.value,
                    error,
                    clientType,
                },
            };

            await kafeProducer.connect();
            await kafeProducer.send({
                topic,
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
