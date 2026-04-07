// src/queue/publisher.js
import { getChannel } from './connection.js';
import { EXCHANGE, QUEUES } from './config.js';

function publish(routingKey, payload) {
  try {
    const channel = getChannel();
    const ok = channel.publish(
      EXCHANGE,
      routingKey,
      Buffer.from(JSON.stringify(payload)),
      { persistent: true, contentType: 'application/json', timestamp: Date.now() }
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
 * @param {object[]} rows  - array of mapped telemetry objects
 */
export function publishTelemetryBatch(rows) {
  return publish(QUEUES.TELEMETRY.routingKey, { batch: rows });
}

/**
 * Publish a SINGLE telemetry payload from a real-time device HTTP POST.
 * Wrapped in the same { batch } envelope so the consumer handles both uniformly.
 *
 * @param {string} deviceId
 * @param {object} data  - { latitude, longitude, speed, ignition, recordedAt }
 */
export function publishTelemetry(deviceId, data) {
  return publish(QUEUES.TELEMETRY.routingKey, {
    batch: [{
      deviceId,
      latitude:   data.latitude   ?? null,
      longitude:  data.longitude  ?? null,
      speed:      data.speed      ?? null,
      ignition:   data.ignition   ?? false,
      signalTime: data.recordedAt ?? new Date().toISOString(),
    }],
  });
}

/**
 * Publish an alert event.
 *
 * @param {string} deviceId
 * @param {object} alert  - { type, severity, message }
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
