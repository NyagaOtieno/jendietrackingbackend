// src/queue/publisher.js
import { getChannel } from './connection.js';
import { EXCHANGE, QUEUES } from './config.js';

/**
 * Safe JSON serializer to handle BigInt values
 */
function safeStringify(payload) {
  return JSON.stringify(payload, (_, value) =>
    typeof value === 'bigint' ? value.toString() : value
  );
}

function publish(routingKey, payload) {
  try {
    const channel = getChannel();

    const ok = channel.publish(
      EXCHANGE,
      routingKey,
      Buffer.from(safeStringify(payload)), // ✅ FIX APPLIED HERE
      {
        persistent: true,
        contentType: 'application/json',
        timestamp: Date.now()
      }
    );

    if (!ok) console.warn(`[Publisher] Backpressure on: ${routingKey}`);
    return ok;
  } catch (err) {
    // Channel not ready yet (RabbitMQ still starting)
    console.error('[Publisher] Could not publish — queue not ready:', err.message);
    return false;
  }
}

/**
 * Publish a BATCH of telemetry rows from the MariaDB sync.
 * One message = up to INSERT_BATCH rows (default 100).
 * Primary publish path — called by mariaSync.service.js.
 *
 * @param {object[]} rows
 */
export function publishTelemetryBatch(rows) {
  return publish(QUEUES.TELEMETRY.routingKey, { batch: rows });
}

/**
 * Publish a SINGLE telemetry payload from a real-time device HTTP POST.
 * Wrapped in the same { batch } envelope so the consumer handles both uniformly.
 *
 * @param {string} deviceId
 * @param {object} data
 */
export function publishTelemetry(deviceId, data) {
  return publish(QUEUES.TELEMETRY.routingKey, {
    batch: [{
      deviceId,
      latitude: data.latitude ?? null,
      longitude: data.longitude ?? null,
      speedKph: data.speedKph ?? data.speed ?? 0,
      heading: data.heading ?? null,
      ignition: data.ignition ?? false,
      deviceTime: data.deviceTime ?? new Date().toISOString(),
    }],
  });
}

/**
 * Publish an alert event.
 *
 * @param {string} deviceId
 * @param {object} alert
 */
export function publishAlert(deviceId, alert) {
  return publish(QUEUES.ALERTS.routingKey, {
    deviceId,
    type:       alert.type     || 'unknown',
    severity:   alert.severity || 'info',
    message:    alert.message  || null,
    recordedAt: new Date().toISOString(),
  });
}