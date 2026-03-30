import express from "express";
import { pgPool } from "../config/db.js";
import { deviceAuth } from "../middleware/deviceAuth.js";
import { requireAuth } from "../middleware/auth.js";

const router = express.Router();

// ✅ Admin / Staff: Get latest telemetry per device
router.get("/latest", requireAuth, async (req, res) => {
  try {
    const { limit = 100, accountId, deviceId } = req.query;
    const values = [];
    let where = "";

    if (accountId) {
      values.push(accountId);
      where += `WHERE d.account_id = $${values.length} `;
    }

    if (deviceId) {
      values.push(deviceId);
      where += where ? `AND t.device_id = $${values.length} ` : `WHERE t.device_id = $${values.length} `;
    }

    const query = `
      SELECT DISTINCT ON (t.device_id)
        t.device_id,
        t.latitude,
        t.longitude,
        t.speed,
        t.ignition,
        COALESCE(t.recorded_at, t.signal_time, t.device_time) AS signal_time,
        v.plate_number
      FROM telemetry t
      LEFT JOIN devices d ON d.id = t.device_id
      LEFT JOIN vehicles v ON v.id = d.vehicle_id
      ${where}
      ORDER BY t.device_id, COALESCE(t.recorded_at, t.signal_time, t.device_time) DESC
      LIMIT ${parseInt(limit, 10)}
    `;

    const result = await pgPool.query(query, values);
    res.json({ success: true, data: result.rows });
  } catch (err) {
    console.error("Telemetry fetch error:", err);
    res.status(500).json({ success: false, message: err.message });
  }
});

// ✅ Device: Get its own latest telemetry
router.get("/my/latest", deviceAuth, async (req, res) => {
  try {
    const query = `
      SELECT t.device_id,
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
      LIMIT 1
    `;
    const result = await pgPool.query(query, [req.device.id]);

    res.json({ success: true, data: result.rows[0] || null });
  } catch (err) {
    console.error("Device telemetry fetch error:", err);
    res.status(500).json({ success: false, message: err.message });
  }
});

export default router;