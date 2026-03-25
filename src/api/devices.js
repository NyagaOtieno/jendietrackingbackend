import express from "express";
import { query } from "../config/db.js";
import { generateApiKey } from "../utils/apiKey.js";
import { protect, adminOnly } from "../middleware/auth.middleware.js";

const router = express.Router();

router.post("/", protect, adminOnly, async (req, res) => {
  try {
    const { name, imei, vehicleId, accountId } = req.body;

    const apiKey = generateApiKey();

    const result = await query(
      `
      INSERT INTO devices (name, imei, api_key, vehicle_id, account_id)
      VALUES ($1, $2, $3, $4, $5)
      RETURNING *
      `,
      [name, imei, apiKey, vehicleId, accountId]
    );

    res.json({
      success: true,
      data: result.rows[0],
    });
  } catch (err) {
    console.error("create device error:", err);
    res.status(500).json({ success: false, message: "Failed to create device" });
  }
});

export default router;