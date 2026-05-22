// src/workers/telemetryBufferWorker.js
import dotenv from "dotenv";
dotenv.config();

import {
  syncVehicles,
  runMariaSync,
  runQuickSync,
  mariaPool,
  loadDeviceMap
} from "../services/mariaSync.service.js";

import { pgPool } from "../config/db.js";

// ─────────────────────────────────────────────────────────────
// CONFIG (reduced load for stability on small VPS)
// ─────────────────────────────────────────────────────────────

const QUICK_INTERVAL   = Number(process.env.LIVE_SYNC_INTERVAL || 10_000);  // FIXED (was 4s)
const FULL_INTERVAL    = Number(process.env.SYNC_INTERVAL      || 60_000);  // FIXED (was 30s)
const VEHICLE_INTERVAL = Number(process.env.VEHICLE_SYNC_INTERVAL || 1_800_000);

// ─────────────────────────────────────────────────────────────
// GLOBAL SAFETY LOCK (CRITICAL FIX)
// prevents overlapping MariaDB + Postgres storms
// ─────────────────────────────────────────────────────────────

let globalSyncLock = false;

// individual guards (still useful)
let vehBusy = false;

// ─────────────────────────────────────────────────────────────
// SAFE WRAPPER
// ─────────────────────────────────────────────────────────────

const safe = (name, fn) => async () => {
  try {
    if (globalSyncLock) {
      console.log(`[Worker] Skipped ${name} (global lock active)`);
      return;
    }
    await fn();
  } catch (e) {
    console.error(`[Worker] ${name}:`, e.message);
  }
};

// ─────────────────────────────────────────────────────────────
// VEHICLE SYNC (safe)
// ─────────────────────────────────────────────────────────────

const safeVehicle = safe("vehicleSync", async () => {
  if (vehBusy) return;
  vehBusy = true;
  try {
    await syncVehicles();
  } finally {
    vehBusy = false;
  }
});

// ─────────────────────────────────────────────────────────────
// QUICK SYNC (LATEST POSITIONS)
// ─────────────────────────────────────────────────────────────

const safeQuick = safe("quickSync", async () => {
  globalSyncLock = true;
  try {
    await runQuickSync();
  } finally {
    globalSyncLock = false;
  }
});

// ─────────────────────────────────────────────────────────────
// FULL SYNC (MARIADB → POSTGRES)
// ─────────────────────────────────────────────────────────────

const safeFull = safe("MariaSync", async () => {
  globalSyncLock = true;
  try {
    await runMariaSync();
  } finally {
    globalSyncLock = false;
  }
});

// ─────────────────────────────────────────────────────────────
// ERROR HANDLERS
// ─────────────────────────────────────────────────────────────

process.on("uncaughtException", (e) =>
  console.error("[Worker] Uncaught:", e.message)
);

process.on("unhandledRejection", (e) =>
  console.error("[Worker] Rejection:", e)
);

// ─────────────────────────────────────────────────────────────
// STARTUP
// ─────────────────────────────────────────────────────────────

async function start() {
  try {
    // sanity checks
    const pg = await pgPool.connect();
    pg.release();
    console.log("✅ PostgreSQL connected");

    const mc = await mariaPool.getConnection();
    mc.release();
    console.log("✅ MariaDB connected");

    // ensure sync infra exists
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS sync_checkpoints (
        key TEXT PRIMARY KEY,
        value TEXT,
        updated_at TIMESTAMPTZ DEFAULT NOW()
      );

      CREATE TABLE IF NOT EXISTS sync_locks (
        key TEXT PRIMARY KEY,
        locked_at TIMESTAMPTZ DEFAULT NOW()
      );
    `).catch(() => {});

    // ─────────────────────────────
    // 1. VEHICLE SYNC (slow)
    // ─────────────────────────────

    await safeVehicle();
    setInterval(safeVehicle, VEHICLE_INTERVAL);

    // ─────────────────────────────
    // 2. FULL SYNC
    // ─────────────────────────────

    setTimeout(() => {
      console.log(`[Worker] Full sync every ${FULL_INTERVAL / 1000}s`);
      safeFull();
      setInterval(safeFull, FULL_INTERVAL);
    }, 5000);

    // ─────────────────────────────
    // 3. QUICK SYNC
    // ─────────────────────────────

    setTimeout(() => {
      console.log(`[Worker] Quick sync every ${QUICK_INTERVAL / 1000}s`);
      safeQuick();
      setInterval(safeQuick, QUICK_INTERVAL);
    }, 20000);

    // ─────────────────────────────
    // POOL MONITOR (DEBUG)
    // ─────────────────────────────

    setInterval(() => {
      console.log("[POOL STATUS]", {
        maria_total: mariaPool.totalConnections?.(),
        maria_active: mariaPool.activeConnections?.(),
        maria_idle: mariaPool.idleConnections?.(),
      });
    }, 30000);

  } catch (e) {
    console.error("[Worker] Fatal startup error:", e.message);

    // IMPORTANT: retry instead of killing PM2 loop
    setTimeout(start, 30000);
  }
}

// ─────────────────────────────────────────────────────────────

start();