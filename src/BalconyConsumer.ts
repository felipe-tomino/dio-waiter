import { KafkaConsumer } from 'node-rdkafka';

const TOPIC_NAME = 'balcony';
const groupId = 'Waiter';

export default class BalconyConsumer extends KafkaConsumer {
  constructor() {
    super({
      'group.id': groupId,
      'metadata.broker.list': process.env.KAFKA_BROKER_URI || 'localhost:9092',
    }, {})
    super
      .on('ready', () => {
        super.subscribe([TOPIC_NAME]);
        super.consume();
        console.log(`Started ${groupId} consumer`);
      })
      .on('rebalance', () => console.log(`Rebalancing ${groupId} Consumers...`))
      .on('data', ({ value }) => {
        const { table, ...rest } = JSON.parse(value.toString());
        console.log(`Delivering table ${table} order: ${JSON.stringify(Object.values(rest)[0])}`)}
      );
  }

  start() {
    super.connect();
  }

  close() {
    super.disconnect();
  }
}
