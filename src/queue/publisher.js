// src/queue/publisher.js

import { getChannel } from './connection.js';
import { EXCHANGE, QUEUES } from './config.js';

/**
 * Deep-safe JSON serializer (handles BigInt anywhere)
 */
function safePayload(payload) {
  return JSON.parse(
    JSON.stringify(payload, (_, value) =>
      typeof value === 'bigint' ? Number(value) : value
    )
  );
}

function publish(routingKey, payload) {
  try {
    const channel = getChannel();

    const safe = safePayload(payload); // ✅ ALWAYS sanitize

    const ok = channel.publish(
      EXCHANGE,
      routingKey,
      Buffer.from(JSON.stringify(safe)),
      {
        persistent: true,
        contentType: 'application/json',
        timestamp: Date.now()
      }
    );

    if (!ok) console.warn(`[Publisher] Backpressure on: ${routingKey}`);

    return ok;
  } catch (err) {
    console.error('[Publisher] Could not publish — queue not ready:', err.message);
    return false;
  }
}

/**
 * Batch telemetry (Maria sync)
 */
export function publishTelemetryBatch(rows) {
  return publish(QUEUES.TELEMETRY.routingKey, {
    batch: rows
  });
}

/**
 * Single telemetry (HTTP device)
 */
export function publishTelemetry(deviceId, data) {
  return publish(QUEUES.TELEMETRY.routingKey, {
    batch: [{
      deviceId: Number(deviceId),
      latitude:   data.latitude   ?? null,
      longitude:  data.longitude  ?? null,
      speed:      data.speed      ?? null,
      ignition:   data.ignition   ?? false,
      signalTime: data.recordedAt ?? new Date().toISOString(),
    }]
  });
}

/**
 * Alerts
 */
export function publishAlert(deviceId, alert) {
  return publish(QUEUES.ALERTS.routingKey, {
    deviceId: Number(deviceId),
    type:       alert.type     || 'unknown',
    severity:   alert.severity || 'info',
    message:    alert.message  || null,
    recordedAt: new Date().toISOString(),
  });
}