import express from "express";
import { requireAuth } from "../middleware/auth.js";
import { createUser, getUsers } from "../controllers/users.controller.js";

const router = express.Router();

// ALL AUTHENTICATED USERS
router.get("/", requireAuth, getUsers);

// USER CREATION FLOW (ROLE CONTROLLED)
router.post("/", requireAuth, createUser);

export default router;