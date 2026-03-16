import express from "express";
import {
  getFleetDevices,
  getFleets,
  getFleetById,
  createFleet,
  updateFleet,
  deleteFleet
} from "../controllers/fleet.controller.js";

const router = express.Router();

router.get("/", getFleets);
router.get("/:id", getFleetById);
router.post("/", createFleet);
router.put("/:id", updateFleet);
router.delete("/:id", deleteFleet);

router.get("/devices", getFleetDevices);

export default router;