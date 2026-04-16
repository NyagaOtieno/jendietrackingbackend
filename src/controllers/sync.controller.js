import { runMariaSync } from "../services/mariaSync.service.js";

export async function triggerMariaSync(req, res) {
  try {
    if (global.__MARIASYNC_RUNNING__) {
      return res.status(429).json({
        success: false,
        message: "Sync already running",
      });
    }

    const start = Date.now();

    await runMariaSync();

    return res.json({
      success: true,
      duration: `${(Date.now() - start) / 1000}s`,
    });

  } catch (err) {
    return res.status(500).json({ success: false, message: err.message });
  }
}