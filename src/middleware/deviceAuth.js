import { query } from "../config/db.js";

const cache = new Map();
const CACHE_TTL = 60 * 1000;

export const deviceAuth = async (req, res, next) => {
  try {
    const apiKey = req.headers["x-api-key"];

    if (!apiKey) {
      return res.status(401).json({
        success: false,
        message: "Missing API key",
      });
    }

    // ✅ Check cache first
    const cached = cache.get(apiKey);

    if (cached && cached.expiry > Date.now()) {
      req.device = cached.device;
      return next();
    }

    // ✅ DB fallback
    const result = await query(
      `
      SELECT id, account_id, status
      FROM devices
      WHERE api_key = $1
      LIMIT 1
      `,
      [apiKey]
    );

    const device = result.rows[0];

    if (!device || device.status !== "active") {
      return res.status(401).json({
        success: false,
        message: "Invalid API key",
      });
    }

    // ✅ Store in cache
    cache.set(apiKey, {
      device,
      expiry: Date.now() + CACHE_TTL,
    });

    req.device = device;

    next();
  } catch (err) {
    console.error("deviceAuth error:", err);
    res.status(500).json({
      success: false,
      message: "Auth error",
    });
  }
};