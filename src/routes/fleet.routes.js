import express from "express";
import { getFleetDevices } from "../controllers/fleet.controller.js";
import { requireAuth } from "../middleware/auth.js";

const router = express.Router();

router.get("/devices", requireAuth, getFleetDevices);

export default router;