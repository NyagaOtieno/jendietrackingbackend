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
    const b = i * 24;
    return `(${Array.from({ length: 24 }, (_, j) => `$${b + j + 1}`).join(',')})`;
  }).join(',');

  const values = rows.flatMap(r => [
    r.deviceId,
    r.protocol                        || null,
    r.signalTime   ? new Date(r.signalTime)  : new Date(),   // signal_time (NOT NULL)
    r.deviceTime   ? new Date(r.deviceTime)  : null,
    r.fixTime      ? new Date(r.fixTime)     : null,
    r.valid                           ?? false,
    Number(r.latitude)                || 0,
    Number(r.longitude)               || 0,
    r.altitude     != null ? Number(r.altitude)    : null,
    r.speed        != null ? Number(r.speed)        : 0,
    r.course       != null ? Number(r.course)       : null,
    r.address                         || null,
    r.attributes                      || null,
    r.accuracy     != null ? Number(r.accuracy)    : null,
    r.network                         || null,
    r.statuscode                      ?? false,
    r.alarmcode                       || null,
    r.speedlimit   != null ? Number(r.speedlimit)  : null,
    r.odometer     != null ? Number(r.odometer)    : null,
    r.isRead                          ?? false,
    r.signalwireconnected             ?? false,
    r.powerwireconnected              ?? false,
    r.eactime      ? new Date(r.eactime)     : null,
    new Date(),                                              // created_at
  ]);

  await pgPool.query(
    `INSERT INTO telemetry (
       device_id, protocol, signal_time, device_time, fix_time,
       valid, latitude, longitude, altitude, speed, course,
       address, attributes, accuracy, network, statuscode,
       alarmcode, speedlimit, odometer, isread,
       signalwireconnected, powerwireconnected, eactime, created_at
     ) VALUES ${placeholders}
     ON CONFLICT (device_id, signal_time) DO NOTHING`,
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
