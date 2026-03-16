import express from "express";
import { login, register, getMe } from "../controllers/auth.controller.js";
import { requireAuth, requireRole } from "../middleware/auth.js";

const router = express.Router();

router.post("/login", login);
router.post("/register", requireAuth, requireRole("admin"), register);
router.get("/me", requireAuth, getMe);

export default router;