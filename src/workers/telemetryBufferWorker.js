// src/workers/telemetryBufferWorker.js
import dotenv from "dotenv";
dotenv.config();

import {
  syncVehicles,
  runMariaSync,
  runQuickSync,   // fast bulk-upsert, last 2 min, ~1 s per run
  mariaPool,
} from "../services/mariaSync.service.js";
import { pgPool } from "../config/db.js";

// ─── Intervals ────────────────────────────────────────────────────────────────
const QUICK_INTERVAL   = Number(process.env.LIVE_SYNC_INTERVAL    ||   4_000); // 4 s ← live positions
const FULL_INTERVAL    = Number(process.env.SYNC_INTERVAL          ||  30_000); // 30 s ← history
const VEHICLE_INTERVAL = Number(process.env.VEHICLE_SYNC_INTERVAL || 1_800_000); // 30 min ← registry

let quickBusy = false;
let fullBusy  = false;
let vehBusy   = false;

async function safeQuickSync() {
  if (quickBusy) return;
  quickBusy = true;
  try   { await runQuickSync(); }
  catch (e) { console.error("[Worker] quickSync error:", e.message); }
  finally   { quickBusy = false; }
}

async function safeFullSync() {
  if (fullBusy) return;
  fullBusy = true;
  try   { await runMariaSync(); }
  catch (e) { console.error("[Worker] MariaSync error:", e.message); }
  finally   { fullBusy = false; }
}

async function safeVehicleSync() {
  if (vehBusy) return;
  vehBusy = true;
  try   { await syncVehicles(); }
  catch (e) { console.error("[Worker] Vehicle sync error:", e.message); }
  finally   { vehBusy = false; }
}

process.on("uncaughtException",  (e) => console.error("[Worker] Uncaught:", e.message));
process.on("unhandledRejection", (e) => console.error("[Worker] Unhandled rejection:", e));

async function start() {
  const pg = await pgPool.connect(); pg.release();
  console.log("✅ PostgreSQL connected");

  const mc = await mariaPool.getConnection(); mc.release();
  console.log("MariaDB connected");

  // 1. Vehicle registry — now, then every 30 min
  await safeVehicleSync();
  setInterval(safeVehicleSync, VEHICLE_INTERVAL);

  // 2. Full MariaSync — 5 s after boot, every 30 s
  //    Loads device map (required by quickSync) + telemetry history
  //    Bulk ops: completes in ~3 s instead of 32 min
  setTimeout(() => {
    console.log(`[Worker] Full sync every ${FULL_INTERVAL / 1000}s`);
    safeFullSync();
    setInterval(safeFullSync, FULL_INTERVAL);
  }, 5_000);

  // 3. Quick sync — 20 s after boot (device map loaded by then), every 4 s
  //    Only reads last 2 min from MariaDB, bulk-upserts ~300 rows → ~1 s/run
  //    This is what delivers < 5 s end-to-end latency on the map
  setTimeout(() => {
    console.log(`[Worker] Quick sync every ${QUICK_INTERVAL / 1000}s ← live positions`);
    safeQuickSync();
    setInterval(safeQuickSync, QUICK_INTERVAL);
  }, 20_000);
}

start().catch((e) => {
  console.error("[Worker] Fatal:", e.message);
  process.exit(1);
});