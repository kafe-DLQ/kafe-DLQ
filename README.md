<div align="center">
  <h1>Kafe DLQ</h1>
  <p>A lightweight extension of KafkaJS's client providing failed message support though a Dead Letter Queue implementation.<p>
</div>

## Table of Contents
1. [About the Project](#about-the-project)
2. [Getting Started](#getting-started)
   - [Requirements](#requirements)
   - [Usage](#usage)
   - [When you're ready to use Kafe-DLQ](#when-youre-ready-to-use-Kafe-DLQ)
3. [Contributors](#contributors)
4. [License](#license)


## About the Project
Kafe-DLQ is a node package for KafkaJS developers to debug their dead letter queue messages

## Getting Started
npm install kafe-dlq

### Requirements

Before importing Kafe-DLQ you will need to complete the following steps:

- Have node installed. Kafe-DLQ is built for Node 14+.
- Have kafkajs installed. Kafe-DLQ is build for kafkajs 2.0.0+.
- Have a Kafka Cluster set up, configured and ready to go. You can run one from the CLI or with Docker.

### Usage

1. Run npm install Kafe-DLQ
2. Create a .env file with a KAFKA_BROKERS variable set to the port and address of where you are  hosting your cluster:
    ```
    KAFKA_BROKERS='localhost:9091,localhost:9092,localhost:9093'
    ```
3. Import kafe-dlq into the file where you would normally initialize your Kafka client and instantiate a kafe-dlq client just as you normally would with kafkajs.


```javascript

const { KafeDLQClient } = require('kafe-dlq');
const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'dlq-companion',
    brokers: process.env.KAFKA_BROKERS.split(','),
});
```

4. Create a custom callback to handle any specific edge cases where you would want the messages to auto-fail. If this is not specified, kafe-dlq will use kafkajs' normal error handling methods. Pass the callback (if you want to have one) to the KafeDLQClient class.

```javascript
//customize your callback funtion to identify failed messages
const callback = ((message ) => {
  return parseInt(message) > 0;
});

//instantiate a DLQ client by passing in KafkaJS client and the callback function
const client = new KafeDLQClient(kafka, callback);
const testProducer = client.producer();
```

5. Instantiate a consumer and a producer just as you would with kafkajs (they support the same send and subscribe methods) and start producing and consuming. If a message fails, it will automatically be produced to a 'Dead Letter Queue' topic!

```javascript
testProducer.connect()
  .then(() => testProducer.send({
    topic: 'good',
    messages: [{key: '1', value: '1'}, {key: '2', value: '2'}, {key: '3', value: '3'}]
  }))
  .then(() => testProducer.send({
    topic: 'bad',
    messages: [{key: '1', value: '-666'}, {key: '2', value: '-666'}, {key: '3', value: '3'}]
  }))
  .catch((err) => console.log(err));  
```

## Contributors

- Oliver Zhang| [GitHub](https://github.com/zezang) | [Linkedin](https://www.linkedin.com/in/oliver-zhang91/)
- Yirou Chen | [GitHub](https://github.com/WarmDarkMatter) | [Linkedin](https://www.linkedin.com/in/yirouchen/)
- Jacob Cole| [GitHub](https://github.com/WarmDarkMatter) | [Linkedin](https://www.linkedin.com/in/jacobcole34/)
- Caro Gomez | [GitHub](https://github.com/Caro-Gomez) | [Linkedin](https://www.linkedin.com/in/carolina-llano-g%C3%B3mez/)
- Kpange Kaitibi | [GitHub](https://github.com/KpangeKaitibi) | [Linkedin](https://www.linkedin.com/in/kpange-kaitibi-522b31102/)

## License

This product is licensed under the MIT License without restriction.