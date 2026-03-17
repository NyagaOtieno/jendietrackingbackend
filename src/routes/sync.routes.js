import { Router } from "express";
import { triggerMariaSync } from "../controllers/sync.controller.js";

const router = Router();

router.post("/run", triggerMariaSync);

export default router;