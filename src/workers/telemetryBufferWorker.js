// src/workers/telemetryBufferWorker.js
import dotenv from "dotenv";
dotenv.config();
import {
  syncVehicles,
  runMariaSync,
  mariaPool
} from "../services/mariaSync.service.js";
import { pgPool } from "../config/db.js";

const QUICK_INTERVAL   = Number(process.env.LIVE_SYNC_INTERVAL    ||  4_000);
const FULL_INTERVAL    = Number(process.env.SYNC_INTERVAL          || 30_000);
const VEHICLE_INTERVAL = Number(process.env.VEHICLE_SYNC_INTERVAL || 1_800_000);

const busy = { quick: false, full: false, vehicle: false };

async function safeRun(name, fn, flag) {
  if (busy[flag]) return;
  busy[flag] = true;
  try   { await fn(); }
  catch (e) { console.error(`[Worker] ${name}:`, e.message); }
  finally   { busy[flag] = false; }
}

process.on("uncaughtException",  e => console.error("[Worker] Uncaught:", e.message));
process.on("unhandledRejection", e => console.error("[Worker] Rejection:", String(e)));

async function start() {
  // PostgreSQL — required, retry 5×
  for (let i = 1; i <= 5; i++) {
    try {
      const c = await pgPool.connect();
      c.release();
      console.log("✅ PostgreSQL connected");
      break;
    } catch (e) {
      console.warn(`[Worker] PG attempt ${i}/5:`, e.message);
      if (i === 5) throw new Error("PostgreSQL unavailable after 5 attempts");
      await new Promise(r => setTimeout(r, 3000 * i));
    }
  }

  // MariaDB — NOT fatal. Try 3×, continue regardless.
  // Do NOT throw — the sync functions handle their own errors.
  let mariaOk = false;
  for (let i = 1; i <= 3; i++) {
    try {
      const c = await mariaPool.getConnection();
      c.release();
      console.log("✅ MariaDB connected");
      mariaOk = true;
      break;
    } catch (e) {
      console.warn(`[Worker] MariaDB attempt ${i}/3:`, e.message);
      if (i < 3) await new Promise(r => setTimeout(r, 4000));
    }
  }

  if (!mariaOk) {
    console.warn("[Worker] ⚠  MariaDB not reachable — will retry in sync intervals.");
    console.warn("[Worker]    Check MARIA_DB_HOST / MARIA_DB_PASSWORD in .env");
    // Do NOT throw or exit — keep running and retry every 30 s in runMariaSync
  }

  // Infrastructure tables
  try {
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS sync_checkpoints
        (key TEXT PRIMARY KEY, value TEXT, updated_at TIMESTAMPTZ DEFAULT NOW());
      CREATE TABLE IF NOT EXISTS sync_locks
        (key TEXT PRIMARY KEY, locked_at TIMESTAMPTZ DEFAULT NOW());
    `);
  } catch {}

  // Vehicle registry — now + every 30 min
  safeRun("vehicleSync", syncVehicles, "vehicle");
  setInterval(() => safeRun("vehicleSync", syncVehicles, "vehicle"), VEHICLE_INTERVAL);

  // Full sync — 5 s delay, every 30 s
  setTimeout(() => {
    console.log(`[Worker] Full sync every ${FULL_INTERVAL / 1000}s`);
    safeRun("MariaSync", runMariaSync, "full");
    setInterval(() => safeRun("MariaSync", runMariaSync, "full"), FULL_INTERVAL);
  }, 5_000);

  // Quick sync — 25 s delay (device map must be loaded), every 4 s
  setTimeout(() => {
    console.log(`[Worker] Quick sync every ${QUICK_INTERVAL / 1000}s`);
    safeRun("quickSync", runQuickSync, "quick");
    setInterval(() => safeRun("quickSync", runQuickSync, "quick"), QUICK_INTERVAL);
  }, 25_000);
}

// Only exit if PostgreSQL itself is unreachable
start().catch(e => {
  console.error("[Worker] Fatal (PostgreSQL unreachable):", e.message);
  setTimeout(() => process.exit(1), 10_000);
});
