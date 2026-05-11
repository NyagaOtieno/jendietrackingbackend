// src/routes/telemetry.routes.js
import express from "express";
import { pgPool } from "../config/db.js";
import { deviceAuth } from "../middleware/deviceAuth.js";
import { requireAuth } from "../middleware/auth.js";
import { publishTelemetry, publishAlert } from "../queue/publisher.js";

const router = express.Router();

const isValidLatLng = (lat, lng) => {
  const la = Number(lat), ln = Number(lng);
  return Number.isFinite(la) && Number.isFinite(ln) &&
    la >= -90 && la <= 90 && ln >= -180 && ln <= 180;
};

const normalizeNumber = (v, fallback = 0) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
};

// ─────────────────────────────────────────────────────────────────────────────
// POST /api/telemetry — device push ingest
// ─────────────────────────────────────────────────────────────────────────────
router.post("/", deviceAuth, async (req, res) => {
  try {
    const deviceId = req.device.id;
    const { latitude, longitude, speed, ignition, recordedAt, alert } = req.body;

    if (!isValidLatLng(latitude, longitude)) {
      return res.status(400).json({ success: false, message: "Invalid latitude/longitude" });
    }

    const payload = {
      deviceId,
      latitude:   normalizeNumber(latitude),
      longitude:  normalizeNumber(longitude),
      speedKph:   normalizeNumber(speed),
      ignition:   Boolean(ignition),
      deviceTime: recordedAt || new Date().toISOString(),
    };

    const queued = publishTelemetry(deviceId, payload);

    if (global.io) global.io.emit("vehicle:update", payload);
    if (alert && queued) publishAlert(deviceId, alert);

    // Fallback DB insert if queue unavailable
    if (!queued) {
      try {
        await pgPool.query(
          `INSERT INTO telemetry (device_id, latitude, longitude, speed_kph, heading, device_time)
           VALUES ($1,$2,$3,$4,$5,$6)`,
          [deviceId, payload.latitude, payload.longitude, payload.speedKph, null, payload.deviceTime]
        );
        // Also keep latest_positions fresh for direct-push devices
        await pgPool.query(
          `INSERT INTO latest_positions (device_id, latitude, longitude, speed_kph, heading, device_time, received_at, updated_at)
           VALUES ($1,$2,$3,$4,$5,$6,NOW(),NOW())
           ON CONFLICT (device_id) DO UPDATE SET
             latitude    = EXCLUDED.latitude,
             longitude   = EXCLUDED.longitude,
             speed_kph   = EXCLUDED.speed_kph,
             device_time = EXCLUDED.device_time,
             received_at = NOW(),
             updated_at  = NOW()`,
          [deviceId, payload.latitude, payload.longitude, payload.speedKph, null, payload.deviceTime]
        );
      } catch (dbErr) {
        console.error("[Telemetry fallback insert error]", dbErr.message);
      }
    }

    return res.status(202).json({ success: true, message: "Telemetry received" });
  } catch (err) {
    console.error("Telemetry ingest error:", err);
    return res.status(500).json({ success: false, message: "Internal server error" });
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// GET /api/telemetry/latest
//
// Used by the frontend's fetchHistory() in api/positions.ts:
//   apiClient.get("/telemetry/latest", { params: { vehicleId, limit, from, to } })
//
// ✅ FIX 1: previous JOIN was `v.serial = d.serial` — wrong.
//   Devices link to vehicles via d.vehicle_id = v.id.
//
// ✅ FIX 2: previous version ignored `vehicleId` param from frontend.
//   Added vehicleId → filters by vehicle_id on the device.
//
// ✅ FIX 3: added `from` / `to` date filtering for history range queries.
// ─────────────────────────────────────────────────────────────────────────────
router.get("/latest", requireAuth, async (req, res) => {
  try {
    const {
      limit    = 200,
      vehicleId,           // ← from frontend fetchHistory()
      accountId,
      deviceId,
      from,                // ← ISO timestamp range start
      to,                  // ← ISO timestamp range end
    } = req.query;

    const values = [];
    const where  = [];

    // vehicleId filter (most common — used by history panel)
    if (vehicleId) {
      values.push(vehicleId);
      where.push(`d.vehicle_id = $${values.length}`);
    }

    // accountId filter (used for account-scoped queries)
    if (accountId) {
      values.push(accountId);
      where.push(`v.account_id = $${values.length}`);
    }

    // direct deviceId filter
    if (deviceId) {
      values.push(deviceId);
      where.push(`t.device_id = $${values.length}`);
    }

    // date range (history panel)
    if (from) { values.push(from); where.push(`t.received_at >= $${values.length}`); }
    if (to)   { values.push(to);   where.push(`t.received_at <= $${values.length}`); }

    const whereClause = where.length ? `WHERE ${where.join(" AND ")}` : "";
    const safeLimit   = Math.min(Math.max(parseInt(limit) || 200, 1), 2000);

    // When vehicleId or from/to are provided → history (ordered DESC, not DISTINCT)
    const isHistory = !!(vehicleId || from || to);

    const sql = isHistory
      ? `
          SELECT
            t.id::text        AS id,
            t.device_id,
            d.device_uid,
            t.latitude,
            t.longitude,
            t.speed_kph       AS speed,
            t.heading,
            t.device_time     AS signal_time,
            t.received_at,
            v.plate_number,
            v.serial
          FROM telemetry t
          LEFT JOIN devices  d ON d.id = t.device_id
          LEFT JOIN vehicles v ON v.id = d.vehicle_id
          ${whereClause}
          ORDER BY t.received_at DESC
          LIMIT $${values.length + 1}
        `
      : `
          SELECT DISTINCT ON (t.device_id)
            t.device_id,
            d.device_uid,
            t.latitude,
            t.longitude,
            t.speed_kph       AS speed,
            t.heading,
            t.device_time     AS signal_time,
            t.received_at     AS recorded_at,
            v.plate_number,
            v.serial
          FROM telemetry t
          LEFT JOIN devices  d ON d.id = t.device_id
          LEFT JOIN vehicles v ON v.id = d.vehicle_id
          ${whereClause}
          ORDER BY t.device_id, t.received_at DESC
          LIMIT $${values.length + 1}
        `;

    const result = await pgPool.query(sql, [...values, safeLimit]);

    const cleaned = result.rows
      .map((r) => ({
        ...r,
        latitude:  normalizeNumber(r.latitude,  null),
        longitude: normalizeNumber(r.longitude, null),
        speed:     normalizeNumber(r.speed),
        heading:   normalizeNumber(r.heading),
      }))
      .filter((r) => isValidLatLng(r.latitude, r.longitude));

    return res.json({ success: true, data: cleaned });
  } catch (err) {
    console.error("❌ Latest telemetry error:", err);
    return res.status(500).json({ success: false, message: "Failed to fetch telemetry" });
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// GET /api/telemetry/my/latest — device self-query
// ─────────────────────────────────────────────────────────────────────────────
router.get("/my/latest", deviceAuth, async (req, res) => {
  try {
    const result = await pgPool.query(
      `SELECT * FROM telemetry WHERE device_id = $1 ORDER BY received_at DESC LIMIT 1`,
      [req.device.id]
    );
    return res.json({ success: true, data: result.rows[0] || null });
  } catch (err) {
    console.error("Device telemetry error:", err);
    return res.status(500).json({ success: false, message: "Failed to fetch device telemetry" });
  }
});

export default router;