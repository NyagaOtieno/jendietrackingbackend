import dotenv from "dotenv";
dotenv.config();

/**
 * =========================
 * BIGINT FIX (must be first)
 * =========================
 */
BigInt.prototype.toJSON = function () {
  return this.toString();
};

import express from "express";
import cors from "cors";
import http from "http";

import { initWebSocket } from "./socket/server.js";
import { testDbConnection } from "./config/db.js";
import { initQueue } from "./queue/index.js";
import { runMariaSync } from "./services/mariaSync.service.js";
import { initDb } from "./config/initDb.js";

// routes
import positionsRoutes from "./routes/positions.routes.js";
import fleetRoutes from "./routes/fleet.routes.js";
import authRoutes from "./routes/auth.routes.js";
import seedRoutes from "./routes/seed.routes.js";
import devicesRoutes from "./routes/devices.routes.js";
import accountsRoutes from "./routes/accounts.routes.js";
import vehiclesRoutes from "./routes/vehicles.routes.js";
import syncRoutes from "./routes/sync.routes.js";
import telemetryRoutes from "./routes/telemetry.routes.js";
import usersRoutes from "./routes/users.routes.js";

await initDb();

/**
 * =========================
 * APP + SERVER
 * =========================
 */
const app = express();
const server = http.createServer(app);
const io = initWebSocket(server);

global.io = io;

/**
 * =========================
 * SAFE STATE CONTROL
 * =========================
 */
let mariaRunning = false;
let mariaInterval = null;

/**
 * =========================
 * CORS
 * =========================
 */
app.use(
  cors({
    origin: (origin, callback) => {
      const allowed = [
        "https://trackingfrontend.vercel.app",
        "http://localhost:5173",
        "http://localhost:8080",
        "http://127.0.0.1:5173",
      ];

      if (!origin) return callback(null, true);

      const isAllowed =
        allowed.includes(origin) || origin.endsWith(".vercel.app");

      return callback(null, isAllowed);
    },
    credentials: true,
  })
);

/**
 * =========================
 * MIDDLEWARE
 * =========================
 */
app.use(express.json());

app.use((req, _res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.originalUrl}`);
  next();
});

/**
 * =========================
 * HEALTH CHECK
 * =========================
 */
app.get("/health", async (_req, res) => {
  try {
    await testDbConnection();
    res.json({ success: true, database: "up" });
  } catch {
    res.status(500).json({ success: false, database: "down" });
  }
});

/**
 * =========================
 * ROUTES
 * =========================
 */
app.use("/api/auth", authRoutes);
app.use("/api/seed", seedRoutes);
app.use("/api/accounts", accountsRoutes);
app.use("/api/devices", devicesRoutes);
app.use("/api/positions", positionsRoutes);
app.use("/api/fleet", fleetRoutes);
app.use("/api/vehicles", vehiclesRoutes);
app.use("/api/sync", syncRoutes);
app.use("/api/telemetry", telemetryRoutes);
app.use("/api/users", usersRoutes);

/**
 * =========================
 * ERROR HANDLING
 * =========================
 */
app.use((req, res) => {
  res.status(404).json({
    success: false,
    message: `Route not found: ${req.method} ${req.originalUrl}`,
  });
});

app.use((error, _req, res, _next) => {
  console.error("❌ Error:", error);
  res.status(500).json({
    success: false,
    message: "Internal server error",
  });
});

/**
 * =========================
 * MARIA SYNC ENGINE (FIXED)
 * =========================
 */
function startMariaSync() {
  if (process.env.SYNC_ENABLED !== "true") {
    console.log("⛔ Maria sync disabled");
    return;
  }

  const interval = Number(process.env.SYNC_INTERVAL || 5000);

  const run = async () => {
    if (mariaRunning) return;

    mariaRunning = true;
    try {
      await runMariaSync();
    } catch (e) {
      console.error("❌ MariaSync failed:", e.message);
    } finally {
      mariaRunning = false;
    }
  };

  console.log(`⚡ MariaSync running every ${interval / 1000}s`);

  run();
  mariaInterval = setInterval(run, interval);
}

/**
 * =========================
 * SHUTDOWN
 * =========================
 */
function shutdown(signal) {
  console.log(`🛑 ${signal} received`);

  if (mariaInterval) clearInterval(mariaInterval);

  server.close(() => {
    console.log("✅ Server closed cleanly");
    process.exit(0);
  });
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

/**
 * =========================
 * START SERVER
 * =========================
 */
const PORT = process.env.PORT || 4001;

async function startServer() {
  try {
    await testDbConnection().catch(() => {});

    await initQueue().catch(() => {});

    startMariaSync();

    server.listen(PORT, "0.0.0.0", () => {
      console.log(`🚀 Backend running on port ${PORT}`);
      console.log(`⚡ WebSocket enabled`);
    });
  } catch (err) {
    console.error("❌ Fatal error:", err);
    process.exit(1);
  }
}

startServer();