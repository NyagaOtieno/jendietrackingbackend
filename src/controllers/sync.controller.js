import { runMariaSync } from "../services/mariaSync.service.js";

export async function triggerMariaSync(req, res) {
  try {
    const result = await runMariaSync();
    return res.status(200).json({
      success: true,
      message: "Maria sync completed",
      data: result,
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      message: error?.message || "Maria sync failed",
    });
  }
}