import express from "express";
import {
  getVehicles,
  getVehicleById,
  createVehicle,
  updateVehicle,
  deleteVehicle,
} from "../controllers/vehicles.controller.js";

import {
  requireAuth,
  requirePrivileged, // ✅ THIS IS THE FIX
} from "../middleware/auth.js";

const router = express.Router();

/**
 * =========================
 * PUBLIC (AUTHENTICATED)
 * =========================
 */
router.get("/", requireAuth, getVehicles);
router.get("/:id", requireAuth, getVehicleById);

/**
 * =========================
 * PRIVILEGED (NON-CLIENT)
 * =========================
 */
router.post("/", requireAuth, requirePrivileged, createVehicle);
router.put("/:id", requireAuth, requirePrivileged, updateVehicle);
router.delete("/:id", requireAuth, requirePrivileged, deleteVehicle);

export default router;