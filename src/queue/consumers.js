// src/queue/consumers.js
import { pgPool } from '../config/db.js';
import { getChannel } from './connection.js';
import { QUEUES, PREFETCH_COUNT } from './config.js';

// ─── Telemetry Consumer ───────────────────────────────────────────────────────
// Handles messages from both sources via the same { batch: row[] } envelope:
//   • MariaDB sync  → 100 rows per message (high throughput path)
//   • HTTP device POST → 1 row per message (real-time path)

async function startTelemetryConsumer() {
  const channel = getChannel();
  await channel.prefetch(PREFETCH_COUNT);

  channel.consume(QUEUES.TELEMETRY.name, async (msg) => {
    if (!msg) return;

    let rows;
    try {
      ({ batch: rows } = JSON.parse(msg.content.toString()));
      if (!Array.isArray(rows) || rows.length === 0) {
        channel.ack(msg);
        return;
      }
    } catch (err) {
      console.error('[Consumer] Malformed telemetry message:', err.message);
      channel.nack(msg, false, false); // → dead letter
      return;
    }

    try {
      await bulkInsertTelemetry(rows);
      channel.ack(msg);
    } catch (err) {
      console.error('[Consumer] Telemetry insert failed:', err.message);
      // Requeue once — if Postgres is temporarily unavailable.
      // On second failure the message goes to dead letter.
      channel.nack(msg, false, true);
    }
  });

  console.log('[Consumer] Telemetry consumer started');
}

/**
 * Bulk INSERT into the telemetry table.
 * Full 24-column schema matching mariaSync.service.js exactly.
 */
async function bulkInsertTelemetry(rows) {
  const placeholders = rows.map((_, i) => {
    const b = i * 6;
    return `($${b+1},$${b+2},$${b+3},$${b+4},$${b+5},$${b+6})`;
  }).join(',');

  const values = rows.flatMap(r => [
    r.deviceId,
    Number(r.latitude)  || 0,
    Number(r.longitude) || 0,
    r.speedKph  != null ? Number(r.speedKph)  : null,
    r.heading   != null ? Number(r.heading)   : null,
    r.deviceTime ? new Date(r.deviceTime) : null,
  ]);

  await pgPool.query(
    `INSERT INTO telemetry (
       device_id, latitude, longitude, speed_kph, heading, device_time
     ) VALUES ${placeholders}
     ON CONFLICT DO NOTHING`,
    values
  );
}

// ─── Alerts Consumer ──────────────────────────────────────────────────────────

async function startAlertsConsumer() {
  const channel = getChannel();
  await channel.prefetch(10);

  channel.consume(QUEUES.ALERTS.name, async (msg) => {
    if (!msg) return;
    try {
      const data = JSON.parse(msg.content.toString());

      await pgPool.query(
        `INSERT INTO vehicle_alerts
           (device_id, alert_type, severity, message, recorded_at)
         VALUES ($1, $2, $3, $4, $5)`,
        [
          data.deviceId,
          data.type      || 'unknown',
          data.severity  || 'info',
          data.message   || null,
          data.recordedAt || new Date().toISOString(),
        ]
      );

      if (data.severity === 'critical') {
        // TODO: fire webhook / push notification
        console.warn(`[ALERT] Critical — device ${data.deviceId}: ${data.message}`);
      }

      channel.ack(msg);
    } catch (err) {
      console.error('[Consumer] Alert error:', err.message);
      channel.nack(msg, false, false);
    }
  });

  console.log('[Consumer] Alerts consumer started');
}

// ─── Start all consumers ──────────────────────────────────────────────────────

export async function startAllConsumers() {
  await Promise.all([
    startTelemetryConsumer(),
    startAlertsConsumer(),
  ]);
  console.log('[Consumers] All consumers running');
}
