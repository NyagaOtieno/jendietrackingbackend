// src/controllers/telemetry.controller.js

// ✅ FIX: was missing this import — caused ReferenceError on every ingest call
import { query } from "../config/db.js";

/**
 * POST /api/telemetry
 * Ingest a single telemetry point from a device (HTTP push path).
 *
 * Body: { deviceUid, latitude, longitude, speedKph, heading, deviceTime }
 *
 * ✅ FIX: aligned column names with actual schema:
 *   speed_kph   (was: speed)
 *   heading     (was: course — only in MariaDB source)
 *   device_time (was: recorded_at)
 *   received_at auto-set by DB default
 *   removed:    ignition (column doesn't exist in telemetry table)
 */
export const ingestTelemetry = async (req, res) => {
  try {
    const {
      latitude,
      longitude,
      speedKph = 0,
      heading  = 0,
      deviceTime = null,
    } = req.body;

    const deviceId = req.device?.id;

    if (!deviceId) {
      return res.status(401).json({
        success: false,
        message: "Device not authenticated",
      });
    }

    if (latitude == null || longitude == null) {
      return res.status(400).json({
        success: false,
        message: "latitude and longitude are required",
      });
    }

    // ── Insert into telemetry ────────────────────────────────────────────
    const result = await query(
      `INSERT INTO telemetry (
         device_id,
         latitude,
         longitude,
         speed_kph,
         heading,
         device_time
       )
       VALUES ($1, $2, $3, $4, $5, $6)
       RETURNING
         id::text AS id,
         latitude,
         longitude,
         speed_kph   AS "speedKph",
         heading,
         device_time AS "deviceTime",
         received_at AS "receivedAt"`,
      [
        deviceId,
        Number(latitude),
        Number(longitude),
        Number(speedKph),
        Number(heading),
        deviceTime ? new Date(deviceTime) : null,
      ]
    );

    // ── Upsert latest_positions ──────────────────────────────────────────
    await query(
      `INSERT INTO latest_positions (
         device_id, latitude, longitude,
         speed_kph, heading, device_time, received_at, updated_at
       )
       VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
       ON CONFLICT (device_id) DO UPDATE SET
         latitude    = EXCLUDED.latitude,
         longitude   = EXCLUDED.longitude,
         speed_kph   = EXCLUDED.speed_kph,
         heading     = EXCLUDED.heading,
         device_time = EXCLUDED.device_time,
         received_at = EXCLUDED.received_at,
         updated_at  = NOW()
       WHERE EXCLUDED.device_time >= latest_positions.device_time
          OR latest_positions.device_time IS NULL`,
      [
        deviceId,
        Number(latitude),
        Number(longitude),
        Number(speedKph),
        Number(heading),
        deviceTime ? new Date(deviceTime) : null,
      ]
    );

    // ── Broadcast via Socket.IO if available ─────────────────────────────
    if (global.io) {
      global.io.emit("telemetry", {
        deviceId,
        latitude,
        longitude,
        speedKph,
        heading,
        deviceTime,
        receivedAt: result.rows[0]?.receivedAt,
      });
    }

    return res.status(201).json({ success: true, data: result.rows[0] });

  } catch (err) {
    console.error("telemetry ingest error:", err);
    return res.status(500).json({
      success: false,
      message: "Failed to ingest telemetry",
    });
  }
};