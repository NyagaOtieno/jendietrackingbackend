// src/workers/telemetryBufferWorker.js
import dotenv from "dotenv";
dotenv.config();

import { syncVehicles, runMariaSync, runQuickSync, mariaPool }
  from "../services/mariaSync.service.js";
import { pgPool } from "../config/db.js";

// ─── Intervals ────────────────────────────────────────────────────────────────
const QUICK_INTERVAL   = Number(process.env.LIVE_SYNC_INTERVAL    ||   4_000);
const FULL_INTERVAL    = Number(process.env.SYNC_INTERVAL          ||  30_000);
const VEHICLE_INTERVAL = Number(process.env.VEHICLE_SYNC_INTERVAL || 1_800_000);

// ─── Guard flags ──────────────────────────────────────────────────────────────
let quickBusy = false, fullBusy = false, vehBusy = false;

async function safeRun(name, fn, busyRef) {
  if (busyRef.value) return;
  busyRef.value = true;
  try   { await fn(); }
  catch (e) { console.error(`[Worker] ${name} error:`, e.message); }
  finally   { busyRef.value = false; }
}

const qBusy = { value: false };
const fBusy = { value: false };
const vBusy = { value: false };

process.on("uncaughtException",  e => console.error("[Worker] Uncaught:", e.message));
process.on("unhandledRejection", e => console.error("[Worker] Unhandled:", String(e)));

// ─── Retry helper ─────────────────────────────────────────────────────────────
async function withRetry(name, fn, maxTries = 5, delayMs = 3000) {
  for (let i = 1; i <= maxTries; i++) {
    try { return await fn(); }
    catch (e) {
      console.warn(`[Worker] ${name} attempt ${i}/${maxTries} failed:`, e.message);
      if (i < maxTries) await new Promise(r => setTimeout(r, delayMs * i));
    }
  }
  throw new Error(`${name} failed after ${maxTries} attempts`);
}

// ─── Start ────────────────────────────────────────────────────────────────────
async function start() {
  // 1. PostgreSQL — required, retry up to 5 times
  await withRetry("PostgreSQL connect", async () => {
    const c = await pgPool.connect();
    c.release();
    console.log("✅ PostgreSQL connected");
  });

  // 2. MariaDB — also required, retry up to 5 times
  //    DO NOT process.exit if this fails — retry instead
  await withRetry("MariaDB connect", async () => {
    const c = await mariaPool.getConnection();
    c.release();
    console.log("MariaDB connected");
  });

  // 3. Ensure infrastructure tables exist (ignore if already present)
  try {
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS sync_checkpoints
        (key TEXT PRIMARY KEY, value TEXT, updated_at TIMESTAMPTZ DEFAULT NOW());
      CREATE TABLE IF NOT EXISTS sync_locks
        (key TEXT PRIMARY KEY, locked_at TIMESTAMPTZ DEFAULT NOW());
    `);
  } catch {}

  // 4. Vehicle registry — now, then every 30 min
  safeRun("vehicleSync", syncVehicles, vBusy);
  setInterval(() => safeRun("vehicleSync", syncVehicles, vBusy), VEHICLE_INTERVAL);

  // 5. Full MariaSync — 5 s after boot (loads deviceMapCache), every 30 s
  setTimeout(() => {
    console.log(`[Worker] Full sync every ${FULL_INTERVAL / 1000}s`);
    safeRun("MariaSync", runMariaSync, fBusy);
    setInterval(() => safeRun("MariaSync", runMariaSync, fBusy), FULL_INTERVAL);
  }, 5_000);

  // 6. Quick sync — 25 s after boot (deviceMapCache loaded by then), every 4 s
  setTimeout(() => {
    console.log(`[Worker] Quick sync every ${QUICK_INTERVAL / 1000}s`);
    safeRun("quickSync", runQuickSync, qBusy);
    setInterval(() => safeRun("quickSync", runQuickSync, qBusy), QUICK_INTERVAL);
  }, 25_000);
}

// Only exit on truly unrecoverable error — not on connection timeouts
start().catch(e => {
  console.error("[Worker] Fatal startup error:", e.message);
  // Wait 10 s before exiting so PM2 doesn't hammer the DB
  setTimeout(() => process.exit(1), 10_000);
});