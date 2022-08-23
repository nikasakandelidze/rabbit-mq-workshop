const express = require("express");

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
    const content = req.params.content;
    await ch.basicPublish(
      "direct_exchange_1",
      `routing_key_${content}`,
      "data",
      { persistent: true }
    );
    resp.end();
  });

  app.listen(port, () => {
    console.log(`Service is listening on port ${port}`);
  });
};

setup();
