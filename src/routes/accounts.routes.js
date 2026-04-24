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
import { ROLES } from "../utils/roles.js";

const router = express.Router();

// ANY AUTH USER
router.get("/", requireAuth, getAccounts);
router.get("/:id", requireAuth, getAccountById);

// ADMIN / STAFF / SUPER_ADMIN ONLY
router.post(
  "/",
  requireAuth,
  requireRole(ROLES.SYSTEM_ADMIN, ROLES.ADMIN, ROLES.STAFF),
  createAccount
);

router.put(
  "/:id",
  requireAuth,
  requireRole(ROLES.SYSTEM_ADMIN, ROLES.ADMIN),
  updateAccount
);

router.delete(
  "/:id",
  requireAuth,
  requireRole(ROLES.SYSTEM_ADMIN, ROLES.ADMIN),
  deleteAccount
);

// USER MANAGEMENT
router.post(
  "/:id/users",
  requireAuth,
  requireRole(ROLES.SYSTEM_ADMIN, ROLES.ADMIN),
  addUserToAccount
);

router.get(
  "/:id/users",
  requireAuth,
  requireRole(ROLES.SYSTEM_ADMIN, ROLES.ADMIN),
  getAccountUsers
);

export default router;