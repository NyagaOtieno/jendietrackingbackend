// src/server.js
import dotenv from 'dotenv';
dotenv.config();

/**
 * =========================
 * BIGINT FIX (must be first)
 * =========================
 */
BigInt.prototype.toJSON = function () {
  return this.toString();
};

import express from 'express';
import cors from 'cors';
import cron from 'node-cron';
import http from 'http';

import { initWebSocket } from './socket/server.js';
import { testDbConnection } from './config/db.js';
import { initQueue } from './queue/index.js';
import { runMariaSync } from './services/mariaSync.service.js';

// routes
import positionsRoutes from './routes/positions.routes.js';
import fleetRoutes from './routes/fleet.routes.js';
import authRoutes from './routes/auth.routes.js';
import seedRoutes from './routes/seed.routes.js';
import devicesRoutes from './routes/devices.routes.js';
import accountsRoutes from './routes/accounts.routes.js';
import vehiclesRoutes from './routes/vehicles.routes.js';
import syncRoutes from './routes/sync.routes.js';
import telemetryRoutes from './routes/telemetry.routes.js';
import usersRoutes from './routes/users.routes.js';
import { initMariaDB } from "./config/initDb.js";

await initMariaDB();
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
 * STATE FLAGS
 * =========================
 */
let isRunning = false;
let cronStarted = false;
global.__MARIASYNC_RUNNING__ = false;

/**
 * =========================
 * CORS
 * =========================
 */
app.use(
  cors({
    origin: (origin, callback) => {
      if (!origin) return callback(null, true);

      const allowed = [
        'https://trackingfrontend.vercel.app',
        'http://localhost:5173',
        'http://localhost:8080',
        'http://127.0.0.1:5173',
      ];

      const isAllowed =
        allowed.includes(origin) || origin.endsWith('.vercel.app');

      return callback(null, true); // safe open API
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
app.get('/health', async (_req, res) => {
  try {
    await testDbConnection();
    res.json({ success: true, database: 'up' });
  } catch {
    res.status(500).json({ success: false, database: 'down' });
  }
});

/**
 * =========================
 * ROOT
 * =========================
 */
app.get('/', (_req, res) => {
  res.send('🚀 Jendie Tracking Backend is running');
});

/**
 * =========================
 * ROUTES
 * =========================
 */
app.use('/api/auth', authRoutes);
app.use('/api/seed', seedRoutes);
app.use('/api/accounts', accountsRoutes);
app.use('/api/devices', devicesRoutes);
app.use('/api/positions', positionsRoutes);
app.use('/api/fleet', fleetRoutes);
app.use('/api/vehicles', vehiclesRoutes);
app.use('/api/sync', syncRoutes);
app.use('/api/telemetry', telemetryRoutes);
app.use('/api/users', usersRoutes);

/**
 * =========================
 * 404 HANDLER
 * =========================
 */
app.use((req, res) => {
  res.status(404).json({
    success: false,
    message: `Route not found: ${req.method} ${req.originalUrl}`,
  });
});

/**
 * =========================
 * ERROR HANDLER
 * =========================
 */
app.use((error, _req, res, _next) => {
  console.error('❌ Error:', error);
  res.status(500).json({
    success: false,
    message: 'Internal server error',
  });
});

/**
 * =========================
 * MARIA SYNC CRON JOB
 * =========================
 */
export function startMariaSyncJob() {
  if (cronStarted) return;

  if (process.env.SYNC_ENABLED !== 'true') {
    console.log('⛔ Maria sync disabled');
    return;
  }

  cronStarted = true;

  const schedule = process.env.SYNC_CRON || '*/5 * * * *';
  console.log(`📦 Maria sync scheduled: ${schedule}`);

  cron.schedule(schedule, async () => {
    if (isRunning || global.__MARIASYNC_RUNNING__) return;

    isRunning = true;
    global.__MARIASYNC_RUNNING__ = true;

    try {
      await runMariaSync();
    } catch (err) {
      console.error('❌ Maria Sync failed:', err.message);
    } finally {
      isRunning = false;
      global.__MARIASYNC_RUNNING__ = false;
    }
  });
}

/**
 * =========================
 * GRACEFUL SHUTDOWN
 * =========================
 */
function shutdown(signal) {
  console.log(`🛑 ${signal} received`);

  server.close(() => {
    console.log('✅ Server closed');
    process.exit(0);
  });
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

/**
 * =========================
 * START SERVER
 * =========================
 */
const PORT = process.env.PORT || 4001;

async function startServer() {
  try {
    try {
      await testDbConnection();
      console.log('✅ Database connected');
    } catch (err) {
      console.log('⚠️ DB warning:', err.message);
    }

    await initQueue().catch(() => {});

    startMariaSyncJob();

    server.listen(PORT, '0.0.0.0', () => {
      console.log(`🚀 Backend running on port ${PORT}`);
      console.log(`⚡ WebSocket enabled`);
    });

  } catch (err) {
    console.error('❌ Fatal error:', err);
    process.exit(1);
  }
}

startServer();