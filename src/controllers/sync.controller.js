import { runMariaSync, isSyncRunning } from "../services/mariaSync.service.js";

export async function triggerMariaSync(_req, res) {
  try {
    console.log("📡 triggerMariaSync hit");

    if (isSyncRunning) {
      return res.status(429).json({
        success: false,
        message: "Sync already running",
      });
    }

    const start = Date.now();

    await runMariaSync();

    return res.json({
      success: true,
      message: "Maria sync completed",
      duration: `${((Date.now() - start) / 1000).toFixed(2)}s`,
    });

  } catch (error) {
    console.error("❌ Maria sync failed:", error);

    return res.status(500).json({
      success: false,
      message: error.message,
    });
  }
}