// src/workers/telemetryBufferWorker.js
import dotenv from "dotenv";
dotenv.config();

import { syncVehicles, runMariaSync, runQuickSync, mariaPool } from "../services/mariaSync.service.js";
import { pgPool } from "../config/db.js";

const QUICK_INTERVAL   = Number(process.env.LIVE_SYNC_INTERVAL    ||   4_000);
const FULL_INTERVAL    = Number(process.env.SYNC_INTERVAL          ||  30_000);
const VEHICLE_INTERVAL = Number(process.env.VEHICLE_SYNC_INTERVAL || 1_800_000);

let quickBusy = false, fullBusy = false, vehBusy = false;

const safe = (name, fn) => async () => {
  try   { await fn(); }
  catch (e) { console.error(`[Worker] ${name}:`, e.message); }
};

const safeQuick   = safe("quickSync",   () => { if (!quickBusy) { quickBusy=true; return runQuickSync().finally(()=>quickBusy=false); } });
const safeFull    = safe("MariaSync",   () => { if (!fullBusy)  { fullBusy=true;  return runMariaSync().finally(()=>fullBusy=false);  } });
const safeVehicle = safe("vehicleSync", () => { if (!vehBusy)   { vehBusy=true;   return syncVehicles().finally(()=>vehBusy=false);   } });

process.on("uncaughtException",  e => console.error("[Worker] Uncaught:", e.message));
process.on("unhandledRejection", e => console.error("[Worker] Rejection:", e));

async function start() {
  const pg = await pgPool.connect(); pg.release();
  console.log("✅ PostgreSQL connected");
  const mc = await mariaPool.getConnection(); mc.release();
  console.log("MariaDB connected");

  // Ensure sync infrastructure tables exist
  await pgPool.query(`
    CREATE TABLE IF NOT EXISTS sync_checkpoints (key TEXT PRIMARY KEY, value TEXT, updated_at TIMESTAMPTZ DEFAULT NOW());
    CREATE TABLE IF NOT EXISTS sync_locks       (key TEXT PRIMARY KEY, locked_at TIMESTAMPTZ DEFAULT NOW());
  `).catch(()=>{});

  // 1. Vehicle registry
  await safeVehicle();
  setInterval(safeVehicle, VEHICLE_INTERVAL);

  // 2. Full sync — 5 s after boot, every 30 s
  setTimeout(() => {
    console.log(`[Worker] Full sync every ${FULL_INTERVAL/1000}s`);
    safeFull();
    setInterval(safeFull, FULL_INTERVAL);
  }, 5_000);

  // 3. Quick sync — 20 s after boot (device map loaded), every 4 s
  setTimeout(() => {
    console.log(`[Worker] Quick sync every ${QUICK_INTERVAL/1000}s`);
    safeQuick();
    setInterval(safeQuick, QUICK_INTERVAL);
  }, 20_000);
}

start().catch(e => { console.error("[Worker] Fatal:", e.message); process.exit(1); });
