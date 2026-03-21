import express from "express";
import { triggerMariaSync } from "../controllers/sync.controller.js";

const router = express.Router();

router.post("/run", triggerMariaSync);

export default router;