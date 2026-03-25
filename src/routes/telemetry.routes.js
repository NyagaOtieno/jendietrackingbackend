// File: src/routes/telemetry.js (or in your server.js)
import express from 'express';
const router = express.Router();
import { pgPool } from '../db/pgPool.js'; // adjust path if needed

// GET latest telemetry per device
router.get('/latest', async (req, res) => {
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
    console.error(err);
    res.status(500).json({ success: false, error: err.message });
  }
});

export default router;