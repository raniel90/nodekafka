const PubNub = require('pubnub');
const kafka = require('kafka-node');

process.setMaxListeners(0);

const pubnub = new PubNub({
  publishKey: 'demo',
  subscribeKey: 'sub-c-b0d14910-0601-11e4-b703-02ee2ddab7fe'
});

const client = new kafka.KafkaClient({
  // kafkaHost: 'localhost:9092'
  kafkaHost: '54.183.249.139:9092'
});
const producer = new kafka.HighLevelProducer(client);

const topic = 'wikipedia2';

producer.on('error', function (err) {
  console.log('PRODUCER ERROR', err);
})

pubnub.addListener({
  message: function (msg) {
    const payload = [
      { topic: topic, messages: JSON.stringify(msg.message) }
    ];

    producer.send(payload, function (err, data) {
      if (!err) console.log('data sended with sucess to Kafka');
    });
  }
});

pubnub.subscribe({
  channels: ['pubnub-wikipedia']
});

createTopic = () => {
  const topicsToCreate = [
    {
      topic: topic,
      partitions: 5,
      replicationFactor: 3,
      configEntries: [
        {
          name: 'compression.type',
          value: 'gzip'
        },
        {
          name: 'min.compaction.lag.ms',
          value: '50'
        }
      ],
      replicaAssignment: [
        {
          partition: 0,
          replicas: [3, 4]
        },
        {
          partition: 1,
          replicas: [2, 1]
        }
      ]
    }];

  client.createTopics(topicsToCreate, (error, result) => {
    if (error) console.log('ERROR ON CREATE TOPIC', error);
  });
}

createTopic();