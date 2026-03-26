import express from "express";
import { pgPool } from "../config/db.js";
import { deviceAuth } from "../middleware/deviceAuth.js";
import { requireAuth, requireRole } from "../middleware/auth.js";

const router = express.Router();

// ✅ Get latest telemetry per device
// Option 1: Admin / Staff view (all devices)
router.get(
  "/latest",
  requireAuth,
  async (req, res) => {
    try {
      // Optional query params: limit, accountId, deviceId
      const { limit = 100, accountId, deviceId } = req.query;
      const values = [];
      let where = "";

      if (accountId) {
        values.push(accountId);
        where += `WHERE d.account_id = $${values.length} `;
      }

      if (deviceId) {
        values.push(deviceId);
        where += values.length ? `AND t.device_id = $${values.length} ` : `WHERE t.device_id = $${values.length} `;
      }

     const query = `
  SELECT DISTINCT ON (t.device_id)
    t.device_id,
    t.latitude,
    t.longitude,
    t.speed,
    t.ignition,
    t.recorded_at AS signal_time
  FROM telemetry t
  ORDER BY t.device_id, t.recorded_at DESC
  LIMIT ${parseInt(limit, 10)}
`;

      const result = await pgPool.query(query, values);

      res.json({ success: true, data: result.rows });
    } catch (err) {
      console.error("Telemetry fetch error:", err);
      res.status(500).json({ success: false, message: err.message });
    }
  }
);

// ✅ Device sends its API key to get its own telemetry
router.get(
  "/my/latest",
  deviceAuth,
  async (req, res) => {
    try {
      const result = await pgPool.query(
        `
        SELECT *
        FROM telemetry
        WHERE device_id = $1
        ORDER BY recorded_at DESC
        LIMIT 1
        `,
        [req.device.id]
      );

      res.json({ success: true, data: result.rows[0] || null });
    } catch (err) {
      console.error("Device telemetry fetch error:", err);
      res.status(500).json({ success: false, message: err.message });
    }
  }
);

export default router;