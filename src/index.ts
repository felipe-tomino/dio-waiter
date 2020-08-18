import express, { Application } from 'express';
import OrderProducer from './OrderProducer';
import BalconyConsumer from './BalconyConsumer';

const PORT = process.env.PORT || 3000;

const app: Application = express();
app.use(express.json())

const orderProducer = new OrderProducer();
orderProducer.start();

const balconyConsumer = new BalconyConsumer();
balconyConsumer.start();

app.post('/order', (req, res) => {
  try {
    orderProducer.sendOrder(req.body);
    res.send('Order sent!');
  } catch (error) {
    res.send(error);
  }
});

app.listen(PORT, () => {
  console.log(`Waiter-${PORT} is listening at http://localhost:${PORT}`)
});

process.on('exit', () => {
  orderProducer.close();
  balconyConsumer.close();
});
