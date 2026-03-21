
import dotenv from "dotenv";
import express from "express";
import cors from "cors";
import { testDbConnection } from "./config/db.js";
import positionsRoutes from "./routes/positions.routes.js";
import fleetRoutes from "./routes/fleet.routes.js";
import authRoutes from "./routes/auth.routes.js";
import seedRoutes from "./routes/seed.routes.js";
import devicesRoutes from "./routes/devices.routes.js";
import accountsRoutes from "./routes/accounts.routes.js";
import vehiclesRoutes from "./routes/vehicles.routes.js";
import syncRoutes from "./routes/sync.routes.js";
import cron from "node-cron";
import { runMariaSync } from "./services/mariaSync.service.js";

let isRunning = false;

export function startMariaSyncJob() {
  if (process.env.SYNC_ENABLED !== "true") {
    console.log("Maria sync job disabled");
    return;
  }

  const schedule = process.env.SYNC_CRON || "*/2 * * * *";

  cron.schedule(schedule, async () => {
    if (isRunning) {
      console.log("Maria sync skipped: previous run still in progress");
      return;
    }

    isRunning = true;

    try {
      console.log("Maria sync started");
      const result = await runMariaSync();
      console.log("Maria sync completed", result);
    } catch (error) {
      console.error("Maria sync failed:", error.message);
    } finally {
      isRunning = false;
    }
  });

  console.log(`Maria sync job scheduled: ${schedule}`);
}

dotenv.config();

const app = express();

app.use(
  cors({
    origin: process.env.FRONTEND_ORIGIN?.split(",").map((s) => s.trim()) || "*",
    credentials: true,
  })
);

app.use(express.json());

app.get("/health", async (_req, res) => {
  let db = "down";

  try {
    await testDbConnection();
    db = "up";
  } catch {
    db = "down";
  }

  res.json({
    success: true,
    message: "Backend is running",
    database: db,
  });
});

app.use("/api/auth", authRoutes);
app.use("/api/seed", seedRoutes);
app.use("/api/accounts", accountsRoutes);
app.use("/api/devices", devicesRoutes);
app.use("/api/positions", positionsRoutes);
app.use("/api/fleet", fleetRoutes);
app.use("/api/vehicles", vehiclesRoutes);
app.use("/api/sync", syncRoutes);

app.use((req, res) => {
  res.status(404).json({
    success: false,
    message: `Route not found: ${req.method} ${req.originalUrl}`,
  });
});

app.use((error, _req, res, _next) => {
  console.error("Unhandled server error:", error);
  res.status(500).json({
    success: false,
    message: "Internal server error",
  });
});

const PORT = process.env.PORT || 4000;

startMariaSyncJob();

app.listen(PORT, "0.0.0.0", () => {
  console.log(`Backend running on port ${PORT}`);
});