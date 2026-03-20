import express from "express";
import {
  getAccounts,
  getAccountById,
  createAccount,
  updateAccount,
  deleteAccount,
  addUserToAccount,
  getAccountUsers,
} from "../controllers/accounts.controller.js";
import { requireAuth, requireRole } from "../middleware/auth.js";

const router = express.Router();

router.get("/", requireAuth, getAccounts);
router.get("/:id", requireAuth, getAccountById);
router.post("/", requireAuth, requireRole("admin"), createAccount);
router.put("/:id", requireAuth, requireRole("admin"), updateAccount);
router.delete("/:id", requireAuth, requireRole("admin"), deleteAccount);

router.post("/:id/users", requireAuth, requireRole("admin"), addUserToAccount);
router.get("/:id/users", requireAuth, requireRole("admin"), getAccountUsers);

export default router;