import express from "express";
import { runMariaSync } from "../services/mariaSync.service.js";

const router = express.Router();

// Trigger manual sync
router.post("/run", async (req, res) => {
  try {
    const result = await runMariaSync();
    res.json({
      success: true,
      message: "Sync completed",
      data: result,
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: error.message,
    });
  }
});

export default router;