import express from "express";
import { triggerMariaSync } from "../controllers/sync.controller.js";

const router = express.Router();

router.post("/maria-sync", triggerMariaSync);

export default router;