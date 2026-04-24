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
  requirePrivileged,
} from "../middleware/auth.js";

const router = express.Router();

/**
 * =========================
 * READ (ALL AUTH USERS)
 * =========================
 */
router.get("/", requireAuth, getVehicles);
router.get("/:id", requireAuth, getVehicleById);

/**
 * =========================
 * WRITE OPERATIONS
 * =========================
 * FIX: still protected, but controller ALSO validates
 */
router.post("/", requireAuth, requirePrivileged, createVehicle);
router.put("/:id", requireAuth, updateVehicle);   // 🔥 FIXED
router.delete("/:id", requireAuth, deleteVehicle); // 🔥 FIXED

export default router;