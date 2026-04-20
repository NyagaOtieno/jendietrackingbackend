import express from 'express';
import { pgPool } from '../config/db.js';
import { deviceAuth } from '../middleware/deviceAuth.js';
import { requireAuth } from '../middleware/auth.js';
import { publishTelemetry, publishAlert } from '../queue/publisher.js';

const router = express.Router();

/**
 * =========================
 * DEVICE INGEST (FAST PATH)
 * =========================
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
    latitude: Number(latitude),
    longitude: Number(longitude),
    speedKph: Number(speed || 0),
    ignition: Boolean(ignition),
    deviceTime: recordedAt || new Date().toISOString(),
  };

  const queued = publishTelemetry(deviceId, payload);

  // realtime push (safe)
  if (global.io) {
    global.io.emit('vehicle:update', payload);
  }

  if (alert && queued) {
    publishAlert(deviceId, alert);
  }

  // fallback DB insert if queue fails
  if (!queued) {
    pgPool.query(
      `INSERT INTO telemetry
        (device_id, latitude, longitude, speed_kph, heading, device_time)
       VALUES ($1,$2,$3,$4,$5,$6)`,
      [
        deviceId,
        payload.latitude,
        payload.longitude,
        payload.speedKph,
        null,
        payload.deviceTime
      ]
    ).catch(err =>
      console.error('[Telemetry fallback insert error]', err.message)
    );
  }

  return res.status(202).json({
    success: true,
    message: 'Telemetry received'
  });
});

/**
 * =========================
 * FIXED: LATEST TELEMETRY (IMPORTANT FIX)
 * =========================
 */
router.get('/latest', requireAuth, async (req, res) => {
  try {
    const { limit = 200, accountId, deviceId } = req.query;

    const values = [];
    const where = [];

    if (accountId) {
      values.push(accountId);
      where.push(`d.account_id = $${values.length}`);
    }

    if (deviceId) {
      values.push(deviceId);
      where.push(`t.device_id = $${values.length}`);
    }

    const whereClause = where.length
      ? `WHERE ${where.join(' AND ')}`
      : '';

    const result = await pgPool.query(
      `
      SELECT DISTINCT ON (t.device_id)
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
      ${whereClause}
      ORDER BY t.device_id, t.device_time DESC
      LIMIT $${values.length + 1}
      `,
      [...values, Math.min(Number(limit) || 200, 500)]
    );

    res.json({
      success: true,
      data: result.rows.map(r => ({
        ...r,
        latitude: Number(r.latitude),
        longitude: Number(r.longitude),
        speed: Number(r.speed || 0),
        heading: Number(r.heading || 0),
      })),
    });

  } catch (err) {
    console.error('❌ Latest telemetry error:', err);
    res.status(500).json({
      success: false,
      message: err.message
    });
  }
});

/**
 * =========================
 * DEVICE SELF QUERY
 * =========================
 */
router.get('/my/latest', deviceAuth, async (req, res) => {
  try {
    const result = await pgPool.query(
      `
      SELECT *
      FROM telemetry
      WHERE device_id = $1
      ORDER BY device_time DESC
      LIMIT 1
      `,
      [req.device.id]
    );

    res.json({
      success: true,
      data: result.rows[0] || null
    });

  } catch (err) {
    console.error('Device telemetry error:', err);
    res.status(500).json({
      success: false,
      message: err.message
    });
  }
});

export default router;