import { runMariaSync } from "../services/mariaSync.service.js";

export async function triggerMariaSync(_req, res) {
  try {
    console.log("triggerMariaSync hit");

    const result = await runMariaSync();

    return res.status(200).json({
      success: true,
      message: "Maria sync completed",
      data: result,
    });
  } catch (error) {
    console.error("Maria sync failed:", error);

    return res.status(500).json({
      success: false,
      message: error?.message || "Maria sync failed",
    });
  }
}