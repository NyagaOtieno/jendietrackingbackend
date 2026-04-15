// src/server.js
import dotenv from 'dotenv';
import express from 'express';
import cors from 'cors';
import { testDbConnection } from './config/db.js';
import { initQueue } from './queue/index.js';

import positionsRoutes  from './routes/positions.routes.js';
import fleetRoutes      from './routes/fleet.routes.js';
import authRoutes       from './routes/auth.routes.js';
import seedRoutes       from './routes/seed.routes.js';
import devicesRoutes    from './routes/devices.routes.js';
import accountsRoutes   from './routes/accounts.routes.js';
import vehiclesRoutes   from './routes/vehicles.routes.js';
import syncRoutes       from './routes/sync.routes.js';
import telemetryRoutes  from './routes/telemetry.routes.js';

import cron from 'node-cron';
import { runMariaSync } from './services/mariaSync.service.js';

dotenv.config();

const app = express();
let isRunning = false;

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

// Optional request logging (helps debugging)
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
  } catch {
    db = 'down';
  }

  res.status(db === 'up' ? 200 : 500).json({
    success: db === 'up',
    message: 'Backend is running',
    database: db,
  });
});

// Root route (useful for Railway)
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

// ─── 404 handler ──────────────────────────────────────────────────────────────
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
  if (process.env.SYNC_ENABLED !== 'true') {
    console.log('Maria sync job disabled');
    return;
  }

  console.log('Maria sync job enabled');

  let schedule = process.env.SYNC_CRON;

  if (!schedule || !/^[\d\*\/,\- ]+$/.test(schedule)) {
    console.warn('Invalid SYNC_CRON value, falling back to safe default');
    schedule = '*/5 * * * *';
  }

  let lastRunAt = null;

cron.schedule(schedule, async () => {
  const now = Date.now();

  // 🔒 Prevent overlap + recover stuck jobs
  if (isRunning) {
    const diff = lastRunAt ? now - lastRunAt : 0;

    if (diff > 5 * 60 * 1000) {
      console.warn('⚠️ Previous sync stuck, resetting lock...');
      isRunning = false;
    } else {
      console.log('⏳ Maria sync skipped: previous run still in progress');
      return;
    }
  }

  isRunning = true;
  lastRunAt = now;

  try {
    console.log('🚀 Maria Sync started');
    await runMariaSync();
    console.log('✅ Maria Sync completed');
  } catch (err) {
    console.error('❌ Maria Sync failed:', err.message);
  } finally {
    isRunning = false;
    lastRunAt = null;
  }
});

  console.log(`Maria sync job scheduled: ${schedule}`);
}

// ─── Start Server (SAFE STARTUP) ───────────────────────────────────────────────
const PORT = process.env.PORT || 4000;

async function startServer() {
  try {
    // 1. Check DB before starting
    try {
      await testDbConnection();
      console.log('✅ Database connected');
    } catch (err) {
      console.error('⚠️ Database connection failed (continuing):', err.message);
    }

    // 2. Start cron job
    startMariaSyncJob();

    // 3. Init RabbitMQ queue
    try {
      await initQueue();
      console.log('✅ Queue initialized');
    } catch (err) {
      console.error('[Queue] Init error:', err.message);
    }

    // 4. Start HTTP server
    app.listen(PORT, '0.0.0.0', () => {
      console.log(`🚀 Backend running on port ${PORT}`);
    });

  } catch (err) {
    console.error('❌ Failed to start server:', err);
    process.exit(1);
  }
}

startServer();