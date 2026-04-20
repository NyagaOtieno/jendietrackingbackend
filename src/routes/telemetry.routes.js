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

  const {
    latitude,
    longitude,
    speed,
    ignition,
    recordedAt,
    alert
  } = req.body;

  if (latitude == null || longitude == null) {
    return res.status(400).json({
      success: false,
      message: 'latitude and longitude are required',
    });
  }

  const payload = {
    deviceId,
    latitude,
    longitude,
    speedKph: speed ?? 0,
    ignition: ignition ?? false,
    deviceTime: recordedAt ?? new Date().toISOString(),
  };

  const queued = publishTelemetry(deviceId, payload);

  // ✅ REAL-TIME FIX (THIS IS WHAT YOU WERE MISSING)
  try {
    global.io?.emit('vehicle:update', payload);
  } catch (e) {}

  if (alert && queued) {
    publishAlert(deviceId, alert);
  }

  if (!queued) {
    pgPool.query(
      `INSERT INTO telemetry (
        device_id, latitude, longitude, speed_kph, heading, device_time
      ) VALUES ($1,$2,$3,$4,$5,$6)`,
      [
        deviceId,
        latitude,
        longitude,
        speed ?? 0,
        null,
        recordedAt ?? new Date().toISOString()
      ]
    ).catch(err =>
      console.error('[Telemetry] Fallback insert failed:', err.message)
    );
  }

  return res.status(202).json({
    success: true,
    message: 'Telemetry received'
  });
});

// ─── Admin / Staff: Get latest telemetry per device (GET) ────────────────────

router.get('/latest', requireAuth, async (req, res) => {
  try {
    const { limit = 100, accountId, deviceId } = req.query;

    const values = [];
    let where = '';

    if (accountId) {
      values.push(accountId);
      where += `AND d.account_id = $${values.length} `;
    }

    if (deviceId) {
      values.push(deviceId);
      where += `AND t.device_id = $${values.length} `;
    }

    values.push(Math.min(parseInt(limit) || 100, 500));

    const result = await pgPool.query(
      `
      SELECT
        t.device_id,
        d.device_uid,
        t.latitude,
        t.longitude,
        t.speed_kph AS speed,
        t.heading,
        t.device_time AS recorded_at,
        v.plate_number,
        v.serial
      FROM telemetry t
      LEFT JOIN devices d ON d.id = t.device_id
      LEFT JOIN vehicles v ON v.serial = d.serial
      WHERE t.id IN (
        SELECT MAX(id)
        FROM telemetry
        GROUP BY device_id
      )
      ${where}
      LIMIT $${values.length}
      `,
      values
    );

    res.json({
      success: true,
      data: result.rows.map(r => ({
        ...r,
        // normalize output (prevents frontend bugs)
        latitude: Number(r.latitude),
        longitude: Number(r.longitude),
        speed: Number(r.speed || 0),
        heading: Number(r.heading || 0),
      })),
    });
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
