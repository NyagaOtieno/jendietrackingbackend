
import express from "express";
import {
  getDevices,
  getDeviceById,
  createDevice,
  updateDevice,
  deleteDevice,
} from "../controllers/devices.controller.js";
import { requireAuth, requireRole } from "../middleware/auth.js";

const router = express.Router();

router.get("/", requireAuth, getDevices);
router.get("/:id", requireAuth, getDeviceById);
router.post("/", requireAuth, requireRole("admin"), createDevice);
router.put("/:id", requireAuth, requireRole("admin"), updateDevice);
router.delete("/:id", requireAuth, requireRole("admin"), deleteDevice);

export default router;
