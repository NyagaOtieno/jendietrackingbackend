import 'dotenv/config';
import amqp from "amqplib";

const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://guest:guest@localhost:5672";

async function testRabbit() {
  try {
    const connection = await amqp.connect(RABBITMQ_URL);
    const channel = await connection.createChannel();

    const queue = "test_queue";
    await channel.assertQueue(queue, { durable: false });

    const msg = "Hello RabbitMQ!";
    channel.sendToQueue(queue, Buffer.from(msg));
    console.log("✅ Sent:", msg);

    channel.consume(queue, (msg) => {
      if (msg !== null) {
        console.log("📥 Received:", msg.content.toString());
        channel.ack(msg);
      }
    });

    setTimeout(() => {
      connection.close();
      console.log("Connection closed.");
      process.exit(0);
    }, 5000);
  } catch (err) {
    console.error("RabbitMQ error:", err);
  }
}

testRabbit();