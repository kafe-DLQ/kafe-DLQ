import { Message } from 'kafkajs';
export declare class KafeDLQClient {
    kafeAdmin: any;
    kafeProducer: any;
    kafeConsumer: any;
    client: any;
    callback?: (message: any) => boolean;
    constructor(client: any, callback?: any);
    producer(): any;
    consumer(groupId: {
        groupId: string;
    }): any;
    sendToDLQ(message: Message, originalTopic: string, clientType: string, err: any): Promise<void>;
    createDLQTopic(topic: string): Promise<void>;
}
