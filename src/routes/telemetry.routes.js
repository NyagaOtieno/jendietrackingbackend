import express from 'express';
import { pgPool } from '../config/db.js';
import { deviceAuth } from '../middleware/deviceAuth.js';
import { requireAuth } from '../middleware/auth.js';
import { publishTelemetry, publishAlert } from '../queue/publisher.js';

const router = express.Router();

/**
 * =========================
 * VALIDATION HELPERS
 * =========================
 */
const isValidLatLng = (lat, lng) => {
  const la = Number(lat);
  const ln = Number(lng);

  return (
    Number.isFinite(la) &&
    Number.isFinite(ln) &&
    la >= -90 &&
    la <= 90 &&
    ln >= -180 &&
    ln <= 180
  );
};

const normalizeNumber = (v, fallback = 0) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
};

/**
 * =========================
 * DEVICE INGEST (FAST PATH)
 * =========================
 */
router.post('/', deviceAuth, async (req, res) => {
  try {
    const deviceId = req.device.id;

    const {
      latitude,
      longitude,
      speed,
      ignition,
      recordedAt,
      alert
    } = req.body;

    // ❗ Critical validation (prevents bad map points like China coordinate)
    if (!isValidLatLng(latitude, longitude)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid latitude/longitude'
      });
    }

    const payload = {
      deviceId,
      latitude: normalizeNumber(latitude),
      longitude: normalizeNumber(longitude),
      speedKph: normalizeNumber(speed),
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
      try {
        await pgPool.query(
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
        );
      } catch (dbErr) {
        console.error('[Telemetry fallback insert error]', dbErr.message);
      }
    }

    return res.status(202).json({
      success: true,
      message: 'Telemetry received'
    });

  } catch (err) {
    console.error('Telemetry ingest error:', err);
    return res.status(500).json({
      success: false,
      message: 'Internal server error'
    });
  }
});

/**
 * =========================
 * FIXED: LATEST TELEMETRY
 * =========================
 */
router.get('/latest', requireAuth, async (req, res) => {
  try {
    const {
      limit = 200,
      accountId,
      deviceId
    } = req.query;

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

    const safeLimit = Math.min(Math.max(parseInt(limit) || 200, 1), 500);

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
      [...values, safeLimit]
    );

    const cleaned = result.rows
      .map(r => ({
        ...r,
        latitude: normalizeNumber(r.latitude, null),
        longitude: normalizeNumber(r.longitude, null),
        speed: normalizeNumber(r.speed),
        heading: normalizeNumber(r.heading),
      }))
      // ❗ removes bad map points (fixes your “wrong location / disappearing vehicles” issue)
      .filter(r => isValidLatLng(r.latitude, r.longitude));

    return res.json({
      success: true,
      data: cleaned
    });

  } catch (err) {
    console.error('❌ Latest telemetry error:', err);
    return res.status(500).json({
      success: false,
      message: 'Failed to fetch telemetry'
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

    return res.json({
      success: true,
      data: result.rows[0] || null
    });

  } catch (err) {
    console.error('Device telemetry error:', err);
    return res.status(500).json({
      success: false,
      message: 'Failed to fetch device telemetry'
    });
  }
});

export default router;