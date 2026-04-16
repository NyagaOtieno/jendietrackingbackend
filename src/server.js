// src/server.js
import dotenv from 'dotenv';
import express from 'express';
import cors from 'cors';
import cron from 'node-cron';

import { testDbConnection } from './config/db.js';
import { initQueue } from './queue/index.js';
import { runMariaSync } from './services/mariaSync.service.js';

import positionsRoutes  from './routes/positions.routes.js';
import fleetRoutes      from './routes/fleet.routes.js';
import authRoutes       from './routes/auth.routes.js';
import seedRoutes       from './routes/seed.routes.js';
import devicesRoutes    from './routes/devices.routes.js';
import accountsRoutes   from './routes/accounts.routes.js';
import vehiclesRoutes   from './routes/vehicles.routes.js';
import syncRoutes       from './routes/sync.routes.js';
import telemetryRoutes  from './routes/telemetry.routes.js';

dotenv.config();

const app = express();

// 🔒 GLOBAL LOCKS
let isRunning = false;
let cronStarted = false;

// ─── CORS ─────────────────────────────────────────────────────────────────────
app.use(
  cors({
    origin: process.env.FRONTEND_ORIGIN
      ? process.env.FRONTEND_ORIGIN.split(',').map(s => s.trim())
      : '*',
    credentials: true,
  })
);

// ─── Middleware ───────────────────────────────────────────────────────────────
app.use(express.json());

app.use((req, _res, next) => {
  console.log(`${req.method} ${req.originalUrl}`);
  next();
});

// ─── Health check ─────────────────────────────────────────────────────────────
app.get('/health', async (_req, res) => {
  let db = 'down';
  try {
    await testDbConnection();
    db = 'up';
  } catch {}

  res.status(db === 'up' ? 200 : 500).json({
    success: db === 'up',
    message: 'Backend is running',
    database: db,
  });
});

app.get('/', (_req, res) => {
  res.send('🚀 Jendie Tracking Backend is running');
});

// ─── Routes ───────────────────────────────────────────────────────────────────
app.use('/api/auth',      authRoutes);
app.use('/api/seed',      seedRoutes);
app.use('/api/accounts',  accountsRoutes);
app.use('/api/devices',   devicesRoutes);
app.use('/api/positions', positionsRoutes);
app.use('/api/fleet',     fleetRoutes);
app.use('/api/vehicles',  vehiclesRoutes);
app.use('/api/sync',      syncRoutes);
app.use('/api/telemetry', telemetryRoutes);

// ─── 404 ──────────────────────────────────────────────────────────────────────
app.use((req, res) => {
  res.status(404).json({
    success: false,
    message: `Route not found: ${req.method} ${req.originalUrl}`,
  });
});

// ─── Error handler ────────────────────────────────────────────────────────────
app.use((error, _req, res, _next) => {
  console.error('Unhandled server error:', error);
  res.status(500).json({
    success: false,
    message: 'Internal server error',
  });
});

// ─── Maria Sync Cron Job ──────────────────────────────────────────────────────
export function startMariaSyncJob() {
  if (cronStarted) {
    console.log('⚠️ Cron already started, skipping duplicate init');
    return;
  }

  if (process.env.SYNC_ENABLED !== 'true') {
    console.log('Maria sync job disabled');
    return;
  }

  cronStarted = true;

  console.log('Maria sync job enabled');

  let schedule = process.env.SYNC_CRON;

  if (!schedule || !/^[\d\*\/,\- ]+$/.test(schedule)) {
    console.warn('Invalid SYNC_CRON value, using default */5 * * * *');
    schedule = '*/5 * * * *';
  }

  let lastRunAt = null;

  cron.schedule(schedule, async () => {
    const now = Date.now();

    // 🔒 Prevent overlap
    if (isRunning) {
      const diff = lastRunAt ? now - lastRunAt : 0;

      if (diff > 5 * 60 * 1000) {
        console.warn('⚠️ Previous sync stuck, resetting lock...');
        isRunning = false;
      } else {
        console.log('⏳ Sync skipped (already running)');
        return;
      }
    }

    isRunning = true;
    lastRunAt = now;

    try {
      await runMariaSync(); // 🚀 SINGLE ENTRY POINT
    } catch (err) {
      console.error('❌ Maria Sync failed:', err.message);
    } finally {
      isRunning = false;
      lastRunAt = null;
    }
  });

  console.log(`Maria sync job scheduled: ${schedule}`);
}

// ─── Graceful shutdown ────────────────────────────────────────────────────────
process.on('SIGINT', () => {
  console.log('🛑 Shutting down server...');
  process.exit(0);
});

process.on('SIGTERM', () => {
  console.log('🛑 Termination signal received...');
  process.exit(0);
});

// ─── Start Server ─────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 4000;

async function startServer() {
  try {
    try {
      await testDbConnection();
      console.log('✅ Database connected');
    } catch (err) {
      console.error('⚠️ DB connection failed (continuing):', err.message);
    }

    startMariaSyncJob();

    try {
      await initQueue();
      console.log('✅ Queue initialized');
    } catch (err) {
      console.error('[Queue] Init error:', err.message);
    }

    app.listen(PORT, '0.0.0.0', () => {
      console.log(`🚀 Backend running on port ${PORT}`);
    });

  } catch (err) {
    console.error('❌ Failed to start server:', err);
    process.exit(1);
  }
}

startServer();