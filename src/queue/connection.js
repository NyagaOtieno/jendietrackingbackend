// src/queue/connection.js
import amqp from 'amqplib';
import { RABBITMQ_URL, EXCHANGE, QUEUES, DEAD_LETTER } from './config.js';

let connection = null;
let channel = null;

export async function connect() {
  try {
    connection = await amqp.connect(RABBITMQ_URL);
    channel = await connection.createChannel();

    // Dead letter exchange — catches nack'd or TTL-expired messages
    await channel.assertExchange(DEAD_LETTER.exchange, 'direct', { durable: true });
    await channel.assertQueue(DEAD_LETTER.queue, { durable: true });
    await channel.bindQueue(DEAD_LETTER.queue, DEAD_LETTER.exchange, '#');

    // Main topic exchange
    await channel.assertExchange(EXCHANGE, 'topic', { durable: true });

    // Assert all queues
    for (const q of Object.values(QUEUES)) {
      await channel.assertQueue(q.name, {
        durable: true,
        arguments: {
          'x-dead-letter-exchange': DEAD_LETTER.exchange,
          'x-dead-letter-routing-key': q.name,
          'x-message-ttl': 60_000, // drop unprocessed messages after 60s
        },
      });
      await channel.bindQueue(q.name, EXCHANGE, q.routingKey);
    }

    connection.on('error', (err) => console.error('[RabbitMQ] Error:', err.message));
    connection.on('close', () => {
      console.warn('[RabbitMQ] Connection closed — reconnecting in 5s...');
      connection = null;
      channel = null;
      setTimeout(connect, 5000);
    });

    console.log('[RabbitMQ] Connected and queues ready');
    return channel;
  } catch (err) {
    console.error('[RabbitMQ] Failed to connect:', err.message, '— retrying in 5s');
    setTimeout(connect, 5000);
  }
}

export function getChannel() {
  if (!channel) throw new Error('RabbitMQ channel not ready — still connecting');
  return channel;
}
