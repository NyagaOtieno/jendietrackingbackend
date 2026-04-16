// src/routes/telemetry.routes.js
import express from 'express';
import { pgPool } from '../config/db.js';
import { deviceAuth } from '../middleware/deviceAuth.js';
import { requireAuth } from '../middleware/auth.js';
import { publishTelemetry, publishAlert } from '../queue/publisher.js';

const router = express.Router();

// ─── Device → Queue (POST) ────────────────────────────────────────────────────

/**
 * POST /api/telemetry
 * Called by GPS devices to push their location data.
 * Authenticated via deviceAuth middleware.
 * Publishes to RabbitMQ — returns 202 immediately without waiting for DB write.
 *
 * Body: {
 *   latitude:   number,
 *   longitude:  number,
 *   speed:      number,      // km/h
 *   ignition:   boolean,
 *   recordedAt: string,      // ISO 8601, optional — defaults to now
 *   alert?: {                // optional, include only if an alert occurred
 *     type:     string,      // e.g. "speeding", "geofence_exit", "harsh_brake"
 *     severity: "info" | "warning" | "critical",
 *     message:  string
 *   }
 * }
 */
router.post('/', deviceAuth, (req, res) => {
  const deviceId = req.device.id;
  const { latitude, longitude, speed, ignition, recordedAt, alert } = req.body;

  if (latitude == null || longitude == null) {
    return res.status(400).json({
      success: false,
      message: 'latitude and longitude are required',
    });
  }

  // Publish telemetry — fire and forget
  const queued = publishTelemetry(deviceId, { latitude, longitude, speed, ignition, recordedAt });

  // Publish alert if present
  if (alert && queued) {
    publishAlert(deviceId, alert);
  }

  if (!queued) {
    // Queue not ready yet (RabbitMQ still starting) — fall back to direct insert
   pgPool.query(
  `INSERT INTO telemetry (
    device_id, latitude, longitude, speed, heading, recorded_at
  ) VALUES ($1,$2,$3,$4,$5,$6)`,
  [
    deviceId,
    latitude,
    longitude,
    speed ?? null,
    null, // heading missing from device input
    recordedAt ?? new Date().toISOString()
  ]
).catch(err => console.error('[Telemetry] Fallback insert failed:', err.message));
  }

  res.status(202).json({ success: true, message: 'Telemetry received' });
});

// ─── Admin / Staff: Get latest telemetry per device (GET) ────────────────────

router.get('/latest', requireAuth, async (req, res) => {
  try {
    const { limit = 100, accountId, deviceId } = req.query;
    const values = [];
    let where = '';

    if (accountId) {
      values.push(accountId);
      where += `WHERE d.account_id = $${values.length} `;
    }

    if (deviceId) {
      values.push(deviceId);
      where += where
        ? `AND t.device_id = $${values.length} `
        : `WHERE t.device_id = $${values.length} `;
    }

 const result = await pgPool.query(
  `SELECT t.device_id,
          t.latitude,
          t.longitude,
          t.speed,
          t.heading,
          t.recorded_at,
          v.plate_number
   FROM telemetry t
   LEFT JOIN devices d ON d.id = t.device_id
   LEFT JOIN vehicles v ON v.id = d.vehicle_id
   WHERE t.device_id = $1
   ORDER BY t.recorded_at DESC
   LIMIT 1`,
  [req.device.id]
);
    res.json({ success: true, data: result.rows });
  } catch (err) {
    console.error('Telemetry fetch error:', err);
    res.status(500).json({ success: false, message: err.message });
  }
});

// ─── Device: Get its own latest telemetry (GET) ───────────────────────────────

router.get('/my/latest', deviceAuth, async (req, res) => {
  try {
    const result = await pgPool.query(
      `SELECT t.device_id,
              t.latitude,
              t.longitude,
              t.speed,
              t.ignition,
              COALESCE(t.recorded_at, t.signal_time, t.device_time) AS signal_time,
              v.plate_number
       FROM telemetry t
       LEFT JOIN devices d ON d.id = t.device_id
       LEFT JOIN vehicles v ON v.id = d.vehicle_id
       WHERE t.device_id = $1
       ORDER BY COALESCE(t.recorded_at, t.signal_time, t.device_time) DESC
       LIMIT 1`,
      [req.device.id]
    );

    res.json({ success: true, data: result.rows[0] || null });
  } catch (err) {
    console.error('Device telemetry fetch error:', err);
    res.status(500).json({ success: false, message: err.message });
  }
});

export default router;
