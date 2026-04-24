import express from "express";
import { requireAuth } from "../middleware/auth.js";

import {
  createUser,
  getUsers,
  updateUser,
  deleteUser,
} from "../controllers/users.controller.js";

const router = express.Router();

// CREATE
router.post("/", requireAuth, createUser);

// READ
router.get("/", requireAuth, getUsers);

// UPDATE
router.put("/:id", requireAuth, updateUser);

// DELETE
router.delete("/:id", requireAuth, deleteUser);

export default router;