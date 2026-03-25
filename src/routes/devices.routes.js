import express from "express";
import {
  getDevices,
  getDeviceById,
  createDevice,
  updateDevice,
  deleteDevice,
} from "../controllers/devices.controller.js";
import { requireAuth, requireRole } from "../middleware/auth.js";
import { z } from "zod";

const router = express.Router();

// ✅ Validation Schemas
const deviceSchema = z.object({
  name: z.string().min(1, "Name is required"),
  imei: z.string().min(10, "IMEI must be at least 10 chars"),
  vehicleId: z.number().int().positive().optional(),
  accountId: z.number().int().positive().optional(),
});

const validate = (schema) => (req, res, next) => {
  const result = schema.safeParse(req.body);

  if (!result.success) {
    return res.status(400).json({
      success: false,
      errors: result.error.errors,
    });
  }

  req.validatedData = result.data;
  next();
};

// ✅ ID param validation (VERY IMPORTANT)
const validateId = (req, res, next) => {
  const id = parseInt(req.params.id);

  if (isNaN(id) || id <= 0) {
    return res.status(400).json({
      success: false,
      message: "Invalid device ID",
    });
  }

  req.params.id = id;
  next();
};

// ✅ Routes
router.get("/", requireAuth, getDevices);

router.get("/:id", requireAuth, validateId, getDeviceById);

router.post(
  "/",
  requireAuth,
  requireRole("admin"),
  validate(deviceSchema),
  createDevice
);

router.put(
  "/:id",
  requireAuth,
  requireRole("admin"),
  validateId,
  validate(deviceSchema.partial()),
  updateDevice
);

router.delete(
  "/:id",
  requireAuth,
  requireRole("admin"),
  validateId,
  deleteDevice
);

export default router;