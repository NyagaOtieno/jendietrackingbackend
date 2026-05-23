// src/workers/telemetryBufferWorker.js
import dotenv from "dotenv";
dotenv.config();

import { syncVehicles, runMariaSync, runQuickSync, mariaPool }
  from "../services/mariaSync.service.js";
import { pgPool } from "../config/db.js";

const QUICK_INTERVAL   = Number(process.env.LIVE_SYNC_INTERVAL    ||   4_000);
const FULL_INTERVAL    = Number(process.env.SYNC_INTERVAL          ||  30_000);
const VEHICLE_INTERVAL = Number(process.env.VEHICLE_SYNC_INTERVAL || 1_800_000);

// ─── Busy guards (objects so refs work across async boundaries) ────────────────
const busy = { quick: false, full: false, vehicle: false };

async function safeRun(name, fn, flag) {
  if (busy[flag]) return;
  busy[flag] = true;
  try   { await fn(); }
  catch (e) { console.error(`[Worker] ${name}:`, e.message); }
  finally   { busy[flag] = false; }
}

// ─── Safety net ───────────────────────────────────────────────────────────────
process.on("uncaughtException",  e => console.error("[Worker] Uncaught:", e.message));
process.on("unhandledRejection", e => console.error("[Worker] Rejection:", String(e)));

// ─── Start ────────────────────────────────────────────────────────────────────
async function start() {

  // 1. PostgreSQL — must be reachable (retry 5×)
  for (let i = 1; i <= 5; i++) {
    try {
      const c = await pgPool.connect();
      c.release();
      console.log("✅ PostgreSQL connected");
      break;
    } catch (e) {
      console.warn(`[Worker] PG attempt ${i}/5:`, e.message);
      if (i === 5) throw e;           // give up → exit → PM2 restarts
      await new Promise(r => setTimeout(r, 3000 * i));
    }
  }

  // 2. MariaDB — NOT fatal if unavailable at startup.
  //    Sync functions will keep retrying on their own interval.
  //    This stops the 1000+ restart crash loop.
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
      if (i < 3) await new Promise(r => setTimeout(r, 5000));
    }
  }

  if (!mariaOk) {
    console.warn("[Worker] ⚠  MariaDB unavailable — running in PG-only mode.");
    console.warn("[Worker]    Check MARIA_HOST/MARIA_USER/MARIA_PASSWORD in .env");
    console.warn("[Worker]    Sync functions will retry automatically.");
  }

  // 3. Ensure infrastructure tables exist
  try {
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS sync_checkpoints
        (key TEXT PRIMARY KEY, value TEXT, updated_at TIMESTAMPTZ DEFAULT NOW());
      CREATE TABLE IF NOT EXISTS sync_locks
        (key TEXT PRIMARY KEY, locked_at TIMESTAMPTZ DEFAULT NOW());
    `);
  } catch {}

  // 4. Vehicle registry — run now (skips silently if MariaDB down), repeat every 30 min
  safeRun("vehicleSync", syncVehicles, "vehicle");
  setInterval(() => safeRun("vehicleSync", syncVehicles, "vehicle"), VEHICLE_INTERVAL);

  // 5. Full MariaSync — 5 s delay (loads deviceMapCache), every 30 s
  setTimeout(() => {
    console.log(`[Worker] Full sync every ${FULL_INTERVAL / 1000}s`);
    safeRun("MariaSync", runMariaSync, "full");
    setInterval(() => safeRun("MariaSync", runMariaSync, "full"), FULL_INTERVAL);
  }, 5_000);

  // 6. Quick sync — 25 s delay (deviceMapCache must be loaded first), every 4 s
  setTimeout(() => {
    console.log(`[Worker] Quick sync every ${QUICK_INTERVAL / 1000}s`);
    safeRun("quickSync", runQuickSync, "quick");
    setInterval(() => safeRun("quickSync", runQuickSync, "quick"), QUICK_INTERVAL);
  }, 25_000);
}

// Only exit when PostgreSQL is unreachable — not for MariaDB issues
start().catch(e => {
  console.error("[Worker] Fatal (PostgreSQL unreachable):", e.message);
  setTimeout(() => process.exit(1), 10_000); // 10 s gap before PM2 restarts
});