import express from "express";
import {
  getVehicles,
  getVehicleById,
  createVehicle,
  updateVehicle,
  deleteVehicle,
} from "../controllers/vehicles.controller.js";
import { requireAuth, requireRole } from "../middleware/auth.js";

const router = express.Router();

router.get("/", requireAuth, getVehicles);
router.get("/:id", requireAuth, getVehicleById);
router.post("/", requireAuth, requireRole("admin"), createVehicle);
router.put("/:id", requireAuth, requireRole("admin"), updateVehicle);
router.delete("/:id", requireAuth, requireRole("admin"), deleteVehicle);

export default router;