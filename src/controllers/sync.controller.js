import { runMariaSync, isSyncRunning } from "../services/mariaSync.service.js";

export async function triggerMariaSync(_req, res) {
  try {
    console.log("📡 triggerMariaSync hit");

    // 🔥 Prevent duplicate execution from API layer
    if (isSyncRunning) {
      return res.status(429).json({
        success: false,
        message: "Sync already running, please wait",
      });
    }

    const startTime = Date.now();

    await runMariaSync();

    const duration = ((Date.now() - startTime) / 1000).toFixed(2);

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