// src/queue/connection.js
import amqp from 'amqplib';
import { RABBITMQ_URL, EXCHANGE, QUEUES, DEAD_LETTER } from './config.js';

let connection = null;
let channel = null;

export async function connect() {
  try {
    connection = await amqp.connect(RABBITMQ_URL);
    channel = await connection.createChannel();

    await channel.assertExchange(DEAD_LETTER.exchange, 'direct', { durable: true });
    await channel.assertQueue(DEAD_LETTER.queue, { durable: true });
    await channel.bindQueue(DEAD_LETTER.queue, DEAD_LETTER.exchange, '#');

    await channel.assertExchange(EXCHANGE, 'topic', { durable: true });

    for (const q of Object.values(QUEUES)) {
      await channel.assertQueue(q.name, {
        durable: true,
        arguments: {
          'x-dead-letter-exchange': DEAD_LETTER.exchange,
          'x-dead-letter-routing-key': q.name,
          'x-message-ttl': 60000,
        },
      });

      await channel.bindQueue(q.name, EXCHANGE, q.routingKey);
    }

    console.log('[RabbitMQ] Connected and ready');
    return channel;

  } catch (err) {
    console.warn('[RabbitMQ] DISABLED (safe mode):', err.message);

    // IMPORTANT: do NOT crash system
    connection = null;
    channel = null;

    return null;
  }
}

export function getChannel() {
  return channel; // allow null safely
}