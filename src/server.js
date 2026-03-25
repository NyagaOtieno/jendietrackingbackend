
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
import { startMariaSyncJob } from "./job/mariaSyncjob.js";
import telemetryRoutes from './routes/telemetry.routes.js';

let isRunning = false;

export function startMariaSyncJob() {
  if (process.env.SYNC_ENABLED !== "true") {
    console.log("Maria sync job disabled");
    return;
  }

  let schedule = process.env.SYNC_CRON;
  if (!schedule || !/^[\d\*\/,\- ]+$/.test(schedule)) {
    console.warn("Invalid SYNC_CRON value, falling back to safe default");
    schedule = "*/5 * * * *";
  }

  cron.schedule(schedule, async () => {
    if (isRunning) {
      console.log("Maria sync skipped: previous run still in progress");
      return;
    }

    isRunning = true;
    try {
      // call the actual sync service, NOT the cron scheduler
      await runMariaSync();
    } catch (err) {
      console.error("Maria sync job failed:", err);
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

// server.js or routes file
app.get('/api/telemetry/latest', async (req, res) => {
  try {
    const result = await pgPool.query(`
      SELECT DISTINCT ON (device_id) 
        device_id, latitude, longitude, signal_time
      FROM telemetry
      ORDER BY device_id, signal_time DESC
    `);
    res.json({ success: true, data: result.rows });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.use("/api/auth", authRoutes);
app.use("/api/seed", seedRoutes);
app.use("/api/accounts", accountsRoutes);
app.use("/api/devices", devicesRoutes);
app.use("/api/positions", positionsRoutes);
app.use("/api/fleet", fleetRoutes);
app.use("/api/vehicles", vehiclesRoutes);
app.use("/api/sync", syncRoutes);
app.use('/api/telemetry', telemetryRoutes);

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