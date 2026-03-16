import express from "express";
import {
  getLatestPositions,
  getHistory,
  createPosition,
  getPositionById,
  updatePosition,
  deletePosition,
} from "../controllers/positions.controller.js";

const router = express.Router();

router.get("/latest", getLatestPositions);
router.get("/history", getHistory);
router.get("/:id", getPositionById);
router.post("/", createPosition);
router.put("/:id", updatePosition);
router.delete("/:id", deletePosition);

export default router;