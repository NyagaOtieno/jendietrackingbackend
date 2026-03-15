import express from "express";
import { seedAdmin } from "../controllers/seed.controller.js";

const router = express.Router();

router.post("/admin", seedAdmin);

export default router;