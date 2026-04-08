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

// CORS
app.use(
  cors({
    origin: process.env.FRONTEND_ORIGIN?.split(',').map(s => s.trim()) || '*',
    credentials: true,
  })
);

// JSON parsing
app.use(express.json());

// Health check
app.get('/health', async (_req, res) => {
  let db = 'down';
  try {
    await testDbConnection();
    db = 'up';
  } catch {
    db = 'down';
  }
  res.json({ success: true, message: 'Backend is running', database: db });
});

// Routes
app.use('/api/auth',      authRoutes);
app.use('/api/seed',      seedRoutes);
app.use('/api/accounts',  accountsRoutes);
app.use('/api/devices',   devicesRoutes);
app.use('/api/positions', positionsRoutes);
app.use('/api/fleet',     fleetRoutes);
app.use('/api/vehicles',  vehiclesRoutes);
app.use('/api/sync',      syncRoutes);
app.use('/api/telemetry', telemetryRoutes);

// 404 handler
app.use((req, res) => {
  res.status(404).json({
    success: false,
    message: `Route not found: ${req.method} ${req.originalUrl}`,
  });
});

// Error handler
app.use((error, _req, res, _next) => {
  console.error('Unhandled server error:', error);
  res.status(500).json({ success: false, message: 'Internal server error' });
});

// ─── Maria Sync Cron Job ──────────────────────────────────────────────────────
export function startMariaSyncJob() {
  if (process.env.SYNC_ENABLED !== 'true') {
    console.log('Maria sync job enabled');
    return;
  }

  let schedule = process.env.SYNC_CRON;
  if (!schedule || !/^[\d\*\/,\- ]+$/.test(schedule)) {
    console.warn('Invalid SYNC_CRON value, falling back to safe default');
    schedule = '*/5 * * * *'; // every 5 minutes
  }

  cron.schedule(schedule, async () => {
    if (isRunning) {
      console.log('Maria sync skipped: previous run still in progress');
      return;
    }
    isRunning = true;
    try {
      console.log('Maria sync job started');
      await runMariaSync();
      console.log('Maria sync job completed');
    } catch (err) {
      console.error('Maria sync job failed:', err);
    } finally {
      isRunning = false;
    }
  });

  console.log(`Maria sync job scheduled: ${schedule}`);
}

// ─── Start ────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 4000;

// Start Maria sync cron
startMariaSyncJob();

// Initialise RabbitMQ queue before HTTP server
// Reads RABBITMQ_URL from env (Fly secret)
initQueue().catch(err => console.error('[Queue] Init error:', err.message));

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Backend running on port ${PORT}`);
});