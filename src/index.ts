import { Kafka } from 'kafkajs';
import * as dotenv from 'dotenv';

import { kafkaMessage } from './types';
import { KafkaConfig, ProducerConfig, Producer } from 'kafkajs';

dotenv.config();

class KafeDLQClient extends Kafka {
    kafeAdmin: any;
    kafeProducer: any;
    kafeConsumer: any;
    dlqTopic: any;

    constructor(configs: KafkaConfig, dlqTopic?: string) {
        super(configs);

        this.kafeAdmin = this.admin();
        this.kafeProducer = this.producer();
        this.dlqTopic = dlqTopic ? dlqTopic : null;
    };

    producer(): any {
        const DLQClient = this;
        const { kafeProducer, dlqTopic } = DLQClient;

        console.log('Original Kafka producer: ', kafeProducer);

        const DLQProducer = {
            ...kafeProducer,
            connect() {
                return kafeProducer.connect();
            },
            async send(topic: string, messages: kafkaMessage[]) {
                try {
                    await kafeProducer.send({
                        topic,
                        messages
                    });
                } catch(err: any) {
                    console.log(err);
                    await this.createDLQTopic();

                    const errObject = {
                        originalTopic: topic,
                        originalMessages: messages,
                        errorMessage: err.message,
                    };

                    this.kafeProducer.send({
                      topic: this.dlqTopic ? this.dlqTopic : 'Dead Letter Queue Message',
                      messages: [
                        {
                            timestamp: Date.now(),
                            value: JSON.stringify(errObject),
                        }],
                    });
                };
            },
        };

        return DLQProducer;
    };

    async createDLQTopic() {
        try {
            await this.kafeAdmin.connect();
            await this.kafeAdmin.createTopics({
              topics: this.dlqTopic ? this.dlqTopic : 'Dead Letter Queue Message',
              numPartitions: 1,
              replicationFactor: 1
            });

            await this.kafeAdmin.disconnect();
        } catch(err) {
            console.log(err);
        };

    };
};

const testClient = new KafeDLQClient({
    clientId: 'test-client',
    brokers: ['localhost:9091', 'localhost:9092', 'localhost:9093']
});

testClient.producer();