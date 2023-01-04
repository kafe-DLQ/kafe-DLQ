import { Message } from 'kafkajs';
export interface producerMessages {
   topic: string,
   messages: Message[],
}

interface consumerSubscribeConfig {
   groupId?: string,
   topic: string[],
   partitionAssigners?: any,
   sessionTimeout?: number,
   rebalanceTimeout?: number,
   heartbeatInterval?: number,
   metadataMaxAge?: number,
   allowAutoTopicCreation?: boolean,
   maxBytesPerPartition?: number,
   minBytes?: number,
   maxBytes?: number,
   maxWaitTimeInMs?: number,
   retry?: object,
   maxInFlightRequests?: number,
   rackId?: string
 }

 interface consumerRunConfig {
   eachMessage: ({
     topic,
     partition,
     message,
   }: {
     topic: string,
     partition: number,
     message: any
   }) => any,
   eachBatchAutoResolve: boolean,
 }