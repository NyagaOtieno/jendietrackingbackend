import express from "express";
import { pgPool } from "../config/db.js";

const router = express.Router();

// GET latest telemetry per device
router.get("/latest", async (req, res) => {
  try {
    const result = await pgPool.query(`
      SELECT DISTINCT ON (device_id) 
        device_id, latitude, longitude, signal_time
      FROM telemetry
      ORDER BY device_id, signal_time DESC
    `);
    res.json({ success: true, data: result.rows });
  } catch (err) {
    console.error("Telemetry fetch error:", err);
    res.status(500).json({ success: false, message: err.message });
  }
});

export default router;