const express = require("express");
const { v4: uuidv4 } = require("uuid");

const app = express();
const port = 8085;

const amqp = require("@cloudamqp/amqp-client");

const messagingAdapter = new amqp.AMQPClient("amqp://localhost:5672");

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
  const amqpConnection = await messagingAdapter.connect();

  const producingChannel = await amqpConnection.channel();

  const consumingChannel = await amqpConnection.channel();

  // Create all needed infrastructure components
  await setupInfrastructure(consumingChannel);

  app.get("/payment/:data", async (req, resp) => {
    const data = req.params.data;
    console.log(`Got HTTP request for /payment/${data}`);

    const uniuqeId = uuidv4();
    const response_queue_name = uniuqeId + "_response_queue";

    console.log(`Creating response queue with name of: ${response_queue_name}`);
    await consumingChannel.queueDeclare(response_queue_name, {
      autoDelete: true,
      exclusive: true,
    }); //since this queue won't have binding it will be bound to direct exchange

    console.log(`Sending message: ${data}  to: 'payments_queue'`);

    await producingChannel.basicPublish(
      "shop_direct_exchange",
      "payment_routing_key",
      JSON.stringify({ response_queue_name: response_queue_name, data: data }),
      { persistent: true }
    );

    const consumer = await consumingChannel.basicConsume(
      response_queue_name,
      { noAck: false },
      async (msg) => {
        const bdy = JSON.parse(msg.bodyString());
        console.log(
          `Payment Producer got response message: ${bdy.data} on responsequeue: ${response_queue_name}`
        );
        if (bdy.queue_name === response_queue_name) {
          console.log("Job done, Canceling consumer");
        } else {
          console.log(`Response queue names don't match`);
        }
        await consumingChannel.basicAck(msg.deliveryTag);
        await consumingChannel.basicCancel(consumer.tag);
      }
    );

    resp.end();
  });

  app.listen(port, () => {
    console.log(`HTTP Server is listening on port ${port}`);
  });
};

setup();
