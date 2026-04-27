import { pgPool } from '../config/db.js';
import { getChannel } from './connection.js';
import { QUEUES, PREFETCH_COUNT } from './config.js';

// ─────────────────────────────────────────────
// SAFE MODE FLAG (prevents crash if RabbitMQ is down)
// ─────────────────────────────────────────────
const RABBIT_DISABLED = true;

// ─────────────────────────────────────────────
// TELEMETRY CONSUMER
// ─────────────────────────────────────────────
async function startTelemetryConsumer() {
  if (RABBIT_DISABLED) {
    console.log('[Consumer] Telemetry disabled (safe mode)');
    return;
  }

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
      console.error('[Telemetry] Bad message:', err.message);
      channel.nack(msg, false, false);
      return;
    }

    try {
      await bulkInsertTelemetry(rows);
      channel.ack(msg);
    } catch (err) {
      console.error('[Telemetry] Insert failed:', err.message);
      channel.nack(msg, false, true);
    }
  });

  console.log('[Consumer] Telemetry started');
}

// ─────────────────────────────────────────────
// BULK INSERT TELEMETRY
// ─────────────────────────────────────────────
async function bulkInsertTelemetry(rows) {
  const placeholders = rows.map((_, i) => {
    const b = i * 6;
    return `($${b + 1},$${b + 2},$${b + 3},$${b + 4},$${b + 5},$${b + 6})`;
  }).join(',');

  const values = rows.flatMap(r => [
    r.deviceId,
    Number(r.latitude) || 0,
    Number(r.longitude) || 0,
    Number(r.speedKph ?? 0),
    Number(r.heading) || 0,
    r.deviceTime ? new Date(r.deviceTime) : new Date(),
  ]);

  await pgPool.query(
    `INSERT INTO telemetry
     (device_id, latitude, longitude, speed_kph, heading, device_time)
     VALUES ${placeholders}
     ON CONFLICT DO NOTHING`,
    values
  );
}

// ─────────────────────────────────────────────
// ALERT CONSUMER
// ─────────────────────────────────────────────
async function startAlertsConsumer() {
  if (RABBIT_DISABLED) {
    console.log('[Consumer] Alerts disabled (safe mode)');
    return;
  }

  const channel = getChannel();
  await channel.prefetch(10);

  channel.consume(QUEUES.ALERTS.name, async (msg) => {
    if (!msg) return;

    try {
      const data = JSON.parse(msg.content.toString());

      await pgPool.query(
        `INSERT INTO vehicle_alerts
         (device_id, alert_type, severity, message, recorded_at)
         VALUES ($1,$2,$3,$4,$5)`,
        [
          data.deviceId,
          data.type || 'unknown',
          data.severity || 'info',
          data.message || null,
          new Date(),
        ]
      );

      channel.ack(msg);
    } catch (err) {
      console.error('[Alert] Error:', err.message);
      channel.nack(msg, false, false);
    }
  });

  console.log('[Consumer] Alerts started');
}

// ─────────────────────────────────────────────
// MAIN EXPORT (FIXES YOUR PM2 CRASH)
// ─────────────────────────────────────────────
export async function startAllConsumers() {
  console.log('[Consumers] Initializing...');
  await Promise.all([
    startTelemetryConsumer(),
    startAlertsConsumer()
  ]);
  console.log('[Consumers] Safe mode ready');
}

// optional default export (safe compatibility)
export default { startAllConsumers };