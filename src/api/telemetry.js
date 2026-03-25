import express from "express";
import { query } from "../config/db.js";
import { deviceAuth } from "../middleware/deviceAuth.js";

const router = express.Router();

router.post("/", deviceAuth, async (req, res) => {
  try {
    const { latitude, longitude, speed, ignition, recordedAt } = req.body;

    const deviceId = req.device.id;

    await query(
      `
      INSERT INTO telemetry (
        device_id,
        latitude,
        longitude,
        speed,
        ignition,
        recorded_at
      )
      VALUES ($1, $2, $3, $4, $5, $6)
      `,
      [
        deviceId,
        latitude,
        longitude,
        speed,
        ignition,
        recordedAt || new Date(),
      ]
    );

    res.json({ success: true });
  } catch (err) {
    console.error("telemetry error:", err);
    res.status(500).json({ success: false, message: "Failed to ingest telemetry" });
  }
});

export default router;