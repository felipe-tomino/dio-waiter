import { Producer } from 'node-rdkafka';

export default class OrderProducer extends Producer {
  constructor() {
    super({
      'metadata.broker.list': process.env.KAFKA_BROKER_URI || 'localhost:9092',
      'dr_cb': true,
    }, {});
    super.on('ready', () => console.log('Started OrderProducer'));
  }

  start() {
    super.connect();
  }

  close() {
    super.disconnect();
  }

  sendOrder(order: { table: number, food: string[], drinks: string[] }) {
    const { table, food, drinks } = order;
    if (food.length) {
      super.produce('food', null, Buffer.from(JSON.stringify({ table, food })));
      console.log('Food order sent to the kitchen!');
    }
    if (drinks.length) {
      super.produce('drinks', null, Buffer.from(JSON.stringify({ table, drinks })));
      console.log('Drinks order sent to the bar!');
    }
  }
}
