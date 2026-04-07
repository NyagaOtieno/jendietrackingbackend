// src/queue/config.js

export const EXCHANGE = 'vehicle_tracking';

export const QUEUES = {
  // Main telemetry queue — GPS, speed, ignition from devices
  TELEMETRY: { name: 'vehicle.telemetry', routingKey: 'vehicle.telemetry' },
  // Alerts queue — critical events, geofence breaches, etc.
  ALERTS:    { name: 'vehicle.alerts',    routingKey: 'vehicle.alerts'    },
};

export const DEAD_LETTER = {
  exchange: 'vehicle_tracking.dlx',
  queue:    'vehicle.dead_letter',
};

export const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://rabbit:rabbit@localhost:5672';

// Max unacknowledged messages per consumer before RabbitMQ stops sending more.
// Tune down if your DB inserts are slow; up if they're fast.
export const PREFETCH_COUNT = 50;

// Rows to accumulate before a bulk INSERT fires
export const BATCH_SIZE = 100;

// Max ms to wait before flushing a partial batch (safety net)
export const BATCH_FLUSH_INTERVAL_MS = 2000;
