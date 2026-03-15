import express from "express";
import { getFleetDevices } from "../controllers/fleet.controller.js";

const router = express.Router();

router.get("/devices", getFleetDevices);

export default router;