import express from "express";
import {
  getLatestPositions,
  getHistory,
} from "../controllers/positions.controller.js";

const router = express.Router();

router.get("/latest", getLatestPositions);
router.get("/history", getHistory);

export default router;