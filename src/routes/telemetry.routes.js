// File: src/routes/telemetry.routes.js
import express from "express";
import { pgPool } from "../config/db.js"; // adjust path

const router = express.Router();

// GET latest telemetry per device
router.get("/latest", async (_req, res) => {
  try {
    const result = await pgPool.query(`
      SELECT DISTINCT ON (device_id)
        device_id,
        latitude,
        longitude,
        signal_time
      FROM telemetry
      ORDER BY device_id, signal_time DESC
    `);

    res.json({ success: true, data: result.rows });
  } catch (err) {
    console.error("Error fetching latest telemetry:", err);
    res.status(500).json({ success: false, error: err.message });
  }
});

export default router;