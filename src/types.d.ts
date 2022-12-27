import { Message } from 'kafkajs';
export interface producerMessages {
   topic: string,
   messages: Message[],
}

export interface failMessages {

}