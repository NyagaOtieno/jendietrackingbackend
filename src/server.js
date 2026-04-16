// src/server.js
import dotenv from 'dotenv';
import express from 'express';
import cors from 'cors';
import cron from 'node-cron';
import http from 'http';

import { initWebSocket } from './socket/server.js';

import { testDbConnection } from './config/db.js';
import { initQueue } from './queue/index.js';
import { runMariaSync } from './services/mariaSync.service.js';

import positionsRoutes from './routes/positions.routes.js';
import fleetRoutes from './routes/fleet.routes.js';
import authRoutes from './routes/auth.routes.js';
import seedRoutes from './routes/seed.routes.js';
import devicesRoutes from './routes/devices.routes.js';
import accountsRoutes from './routes/accounts.routes.js';
import vehiclesRoutes from './routes/vehicles.routes.js';
import syncRoutes from './routes/sync.routes.js';
import telemetryRoutes from './routes/telemetry.routes.js';

dotenv.config();

const app = express();

// =========================
// HTTP SERVER (REQUIRED FOR WS)
// =========================
const server = http.createServer(app);

// =========================
// WEBSOCKET INIT (REALTIME LAYER)
// =========================
const ws = initWebSocket(server);

// =========================
// GLOBAL SAFETY LOCKS
// =========================
let isRunning = false;
let cronStarted = false;

// process-wide PM2 safety
global.__MARIASYNC_RUNNING__ = global.__MARIASYNC_RUNNING__ || false;

// =========================
// CORS
// =========================
app.use(
  cors({
    origin: process.env.FRONTEND_ORIGIN
      ? process.env.FRONTEND_ORIGIN.split(',').map(s => s.trim())
      : '*',
    credentials: true,
  })
);

// =========================
// MIDDLEWARE
// =========================
app.use(express.json());

app.use((req, _res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.originalUrl}`);
  next();
});

// =========================
// HEALTH CHECK
// =========================
app.get('/health', async (_req, res) => {
  try {
    await testDbConnection();
    return res.status(200).json({
      success: true,
      message: 'Backend is running',
      database: 'up',
    });
  } catch (err) {
    return res.status(500).json({
      success: false,
      message: 'Backend is running',
      database: 'down',
    });
  }
});

// =========================
// ROOT
// =========================
app.get('/', (_req, res) => {
  res.send('🚀 Jendie Tracking Backend is running');
});

// =========================
// ROUTES (UNCHANGED)
// =========================
app.use('/api/auth', authRoutes);
app.use('/api/seed', seedRoutes);
app.use('/api/accounts', accountsRoutes);
app.use('/api/devices', devicesRoutes);
app.use('/api/positions', positionsRoutes);
app.use('/api/fleet', fleetRoutes);
app.use('/api/vehicles', vehiclesRoutes);
app.use('/api/sync', syncRoutes);
app.use('/api/telemetry', telemetryRoutes);

// =========================
// 404
// =========================
app.use((req, res) => {
  res.status(404).json({
    success: false,
    message: `Route not found: ${req.method} ${req.originalUrl}`,
  });
});

// =========================
// ERROR HANDLER
// =========================
app.use((error, _req, res, _next) => {
  console.error('❌ Unhandled error:', error);
  res.status(500).json({
    success: false,
    message: 'Internal server error',
  });
});

// =========================
// SAFE CRON JOB
// =========================
export function startMariaSyncJob() {
  if (cronStarted) return;

  if (process.env.SYNC_ENABLED !== 'true') {
    console.log('⛔ Maria sync disabled via ENV');
    return;
  }

  cronStarted = true;

  const schedule =
    process.env.SYNC_CRON && /^[\d\*\/,\- ]+$/.test(process.env.SYNC_CRON)
      ? process.env.SYNC_CRON
      : '*/5 * * * *';

  console.log(`📦 Maria sync scheduled: ${schedule}`);

  cron.schedule(schedule, async () => {
    if (isRunning || global.__MARIASYNC_RUNNING__) {
      console.log('⏳ Sync skipped (already running globally)');
      return;
    }

    isRunning = true;
    global.__MARIASYNC_RUNNING__ = true;

    try {
      console.log('🚀 Maria Sync started');
      await runMariaSync();
      console.log('✅ Maria Sync completed');
    } catch (err) {
      console.error('❌ Maria Sync failed:', err.message);
    } finally {
      isRunning = false;
      global.__MARIASYNC_RUNNING__ = false;
    }
  });
}

// =========================
// GRACEFUL SHUTDOWN (INCLUDING WS)
// =========================
process.on('SIGINT', () => {
  console.log('🛑 SIGINT received');
  server.close(() => process.exit(0));
});

process.on('SIGTERM', () => {
  console.log('🛑 SIGTERM received');
  server.close(() => process.exit(0));
});

// =========================
// START SERVER
// =========================
const PORT = process.env.PORT || 4000;

async function startServer() {
  try {
    try {
      await testDbConnection();
      console.log('✅ Database connected');
    } catch (err) {
      console.log('⚠️ DB connection failed but continuing:', err.message);
    }

    startMariaSyncJob();

    try {
      await initQueue();
      console.log('✅ Queue initialized');
    } catch (err) {
      console.log('⚠️ Queue init failed:', err.message);
    }

    // START HTTP + WS SERVER (IMPORTANT)
    server.listen(PORT, '0.0.0.0', () => {
      console.log(`🚀 Backend running on port ${PORT}`);
      console.log(`⚡ WebSocket enabled for real-time tracking`);
    });

  } catch (err) {
    console.error('❌ Fatal startup error:', err);
    process.exit(1);
  }
}

startServer();