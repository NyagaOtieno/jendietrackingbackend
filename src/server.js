// src/server.js
import dotenv from 'dotenv';
dotenv.config();

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

const app = express();

/**
 * =========================
 * HTTP + WS SERVER
 * =========================
 */
const server = http.createServer(app);
const io = initWebSocket(server); // ✅ SINGLE INIT ONLY

/**
 * expose globally for services (safe pattern)
 */
global.io = io;

/**
 * =========================
 * SAFETY LOCKS
 * =========================
 */
let isRunning = false;
let cronStarted = false;
global.__MARIASYNC_RUNNING__ = global.__MARIASYNC_RUNNING__ || false;

/**
 * =========================
 * CORS
 * =========================
 */
app.use(
  cors({
    origin: process.env.FRONTEND_ORIGIN
      ? process.env.FRONTEND_ORIGIN.split(',').map(s => s.trim())
      : '*',
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
 * HEALTH
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

/**
 * =========================
 * 404
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
  console.error('❌ Unhandled error:', error);
  res.status(500).json({
    success: false,
    message: 'Internal server error',
  });
});

/**
 * =========================
 * CRON JOB (SAFE SINGLE RUN)
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
    if (isRunning || global.__MARIASYNC_RUNNING__) {
      console.log('⏳ Sync skipped (already running)');
      return;
    }

    isRunning = true;
    global.__MARIASYNC_RUNNING__ = true;

    try {
      console.log('🚀 Maria Sync started');
      await runMariaSync(io); // ⚡ PASS IO INTO SYNC (IMPORTANT)
      console.log('✅ Maria Sync completed');
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
process.on('SIGINT', () => {
  console.log('🛑 SIGINT received');
  server.close(() => process.exit(0));
});

process.on('SIGTERM', () => {
  console.log('🛑 SIGTERM received');
  server.close(() => process.exit(0));
});

/**
 * =========================
 * START SERVER
 * =========================
 */
const PORT = process.env.PORT || 4000;

async function startServer() {
  try {
    try {
      await testDbConnection();
      console.log('✅ Database connected');
    } catch (err) {
      console.log('⚠️ DB failed but continuing');
    }

    await initQueue().catch(err =>
      console.log('⚠️ Queue init failed:', err.message)
    );

    startMariaSyncJob();

    server.listen(PORT, '0.0.0.0', () => {
      console.log(`🚀 Backend running on port ${PORT}`);
      console.log(`⚡ WebSocket enabled`);
    });

  } catch (err) {
    console.error('❌ Fatal startup error:', err);
    process.exit(1);
  }
}

startServer();