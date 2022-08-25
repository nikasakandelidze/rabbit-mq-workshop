const amqp = require("@cloudamqp/amqp-client");

var args = process.argv.slice(2);

const QUEUE_TO_LISTEN_FOR = args[0];

const domainToQueueMapping = {
  payment: "payments_queue",
  order: "orders_queue",
};

const setupInfrastructure = async (channel) => {
  // Creating new direct exchange
  await channel.exchangeDeclare("shop_direct_exchange", "direct", {
    durable: false,
  });

  // Bind newly created queus and direct exchange
  await channel.queueDeclare("payments_queue", { durable: true });
  await channel.queueBind(
    "payments_queue",
    "shop_direct_exchange",
    "payment_routing_key"
  );

  await channel.queueDeclare("orders_queue", { durable: true });
  await channel.queueBind(
    "orders_queue",
    "shop_direct_exchange",
    "orders_routing_key"
  );
};

const setup = async () => {
  const messagingAdapter = new amqp.AMQPClient("amqp://localhost");
  // Create single connection to rabbitMQ server
  const connection = await messagingAdapter.connect();

  // Create consuming channel, since in nodejs we have only 1 eventloop thread
  // maybe we won't need many channels ? and we'll complete everything with only 1 ?
  const consumingChannel = await connection.channel();
  const producingChannel = await connection.channel();
  //This is always desirable, since we want each consumer to do 1 job only, and queue scheduler should skip consumer if it has some intensive job to do.
  consumingChannel.prefetch(1);

  await setupInfrastructure(consumingChannel);
  const queueName = domainToQueueMapping[QUEUE_TO_LISTEN_FOR];
  console.log(`Starting up consumer process for queue: ${queueName}`);

  await consumingChannel.basicConsume(
    queueName,
    { noAck: false }, //noAck == no ack needed for completeing task succesfully.
    async (msg) => {
      const body = JSON.parse(msg.bodyString());

      console.log(`Consumer received message: ${msg.bodyString()}`);

      setTimeout(async () => {
        const message = JSON.stringify({
          data: `data: ${body.data} looks good`,
          queue_name: body.response_queue_name,
        });
        console.log(
          `Sending data back to producer: ${body.response_queue_name}`
        );
        await producingChannel.basicPublish(
          "", // default exchange
          body.response_queue_name,
          message
        );
        await consumingChannel.basicAck(msg.deliveryTag);
      }, 5000);
    }
  );
};

setup();
