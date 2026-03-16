import express from "express";
import { getFleets } from "../controllers/fleet.controller.js";
import { requireAuth } from "../middleware/auth.js";

const router = express.Router();

router.get("/devices", requireAuth, getFleets);

export default router;