import express from "express";
import {
  getLatestPositions,
  getHistory,
  createPosition,
  getPositionById,
  updatePosition,
  deletePosition,
} from "../controllers/positions.controller.js";
import { requireAuth, requireRole } from "../middleware/auth.js";

const router = express.Router();

router.get("/latest", requireAuth, getLatestPositions);
router.get("/history", requireAuth, getHistory);
router.get("/:id", requireAuth, getPositionById);
router.post("/", requireAuth, requireRole("admin"), createPosition);
router.put("/:id", requireAuth, requireRole("admin"), updatePosition);
router.delete("/:id", requireAuth, requireRole("admin"), deletePosition);

export default router;