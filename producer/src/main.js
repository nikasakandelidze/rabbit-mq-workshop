const express = require("express");
const { v4: uuidv4 } = require("uuid");

const app = express();
const port = 8085;

const amqp = require("@cloudamqp/amqp-client");

const messagingAdapter = new amqp.AMQPClient("amqp://localhost");

const setup = async () => {
  const conn = await messagingAdapter.connect();
  const ch = await conn.channel();

  // Creating new direct exchange
  await ch.exchangeDeclare("direct_exchange_1", "direct");

  // Bind newly created queus and direct exchange
  await ch.queueDeclare("queue_1", { durable: true });
  await ch.queueBind("queue_1", "direct_exchange_1", "routing_key_1");

  await ch.queueDeclare("queue_2", { durable: true });
  await ch.queueBind("queue_2", "direct_exchange_1", "routing_key_2");

  await ch.queueDeclare("queue_3", { durable: true });
  await ch.queueBind("queue_3", "direct_exchange_1", "routing_key_2");

  app.get("/message/:content", async (req, resp) => {
    console.log("Producing");
    const content = req.params.content;
    const uniuqeId = uuidv4();
    const queue_name = uniuqeId + "_queue";

    await ch.queueDeclare(queue_name, {
      autoDelete: true,
      exclusive: true,
    });

    console.log(
      "Sending name of newly generated response queue:" +
        queue_name +
        "to the consumer"
    );

    await ch.basicPublish(
      "direct_exchange_1",
      `routing_key_${content}`,
      uniuqeId,
      { persistent: true }
    );

    const consumer = await ch.basicConsume(
      queue_name,
      { noAck: false },
      async (msg) => {
        console.log("Producer got message back: " + msg.bodyString());
        console.log("canceling consumer");
        const resultCancel = await ch.basicCancel(consumer.tag);
        console.log(resultCancel);
      }
    );

    resp.end();
  });

  app.listen(port, () => {
    console.log(`Service is listening on port ${port}`);
  });
};

setup();
