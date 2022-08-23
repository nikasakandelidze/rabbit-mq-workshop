const amqp = require("@cloudamqp/amqp-client");

var args = process.argv.slice(2);

const setup = async () => {
  const messagingAdapter = new amqp.AMQPClient("amqp://localhost");
  const conn = await messagingAdapter.connect();
  const ch = await conn.channel();
  await ch.queueDeclare("queue_1");
  await ch.queueDeclare("queue_2");
  await ch.queueDeclare("queue_3");
  ch.prefetch(1);
  await ch.basicConsume(args[0], { noAck: false }, (msg) => {
    console.log(args[0]);
    console.log(msg.bodyToString());
    ch.basicAck(msg.deliveryTag);
  });
};

setup();
