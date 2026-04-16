// src/controllers/sync.controller.js
import { runMariaSync } from "../services/mariaSync.service.js";

export async function triggerMariaSync(_req, res) {
  try {
    console.log("📡 triggerMariaSync hit");

    const startTime = Date.now();

    // 🚀 Run sync (locking handled inside service)
    const result = await runMariaSync();

    const duration = ((Date.now() - startTime) / 1000).toFixed(2);

    // 🔥 If skipped (already running)
    if (result?.skipped) {
      return res.status(429).json({
        success: false,
        message: "Sync already running, please wait",
        duration: `${duration}s`,
      });
    }

    return res.status(200).json({
      success: true,
      message: "Maria sync completed",
      duration: `${duration}s`,
    });

  } catch (error) {
    console.error("❌ Maria sync failed:", error);

    return res.status(500).json({
      success: false,
      message: error?.message || "Maria sync failed",
    });
  }
}