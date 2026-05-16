// src/workers/telemetryBufferWorker.js
import dotenv from "dotenv";
dotenv.config();

import {
  syncVehicles,
  runMariaSync,
  runQuickSync,   // ← fast bulk-upsert, last 2 min, ~1 s
  mariaPool,
} from "../services/mariaSync.service.js";
import { pgPool } from "../config/db.js";

// ── Intervals ─────────────────────────────────────────────────────────────────
const QUICK_INTERVAL   = Number(process.env.LIVE_SYNC_INTERVAL    ||  10_000); // 10 s
const VEHICLE_INTERVAL = Number(process.env.VEHICLE_SYNC_INTERVAL || 1_800_000); // 30 min
const FULL_INTERVAL    = Number(process.env.SYNC_INTERVAL          ||  30_000); // 30 s

// ── Guard flags ───────────────────────────────────────────────────────────────
let quickBusy = false;
let fullBusy  = false;
let vehBusy   = false;

// ── Safe wrappers — no function can crash the process ─────────────────────────
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

// ── Global safety net ─────────────────────────────────────────────────────────
process.on("uncaughtException",  (e) => console.error("[Worker] Uncaught:", e.message));
process.on("unhandledRejection", (e) => console.error("[Worker] Unhandled rejection:", e));

// ── Start ─────────────────────────────────────────────────────────────────────
async function start() {
  // Verify connections
  const pg = await pgPool.connect();
  pg.release();
  console.log("✅ PostgreSQL connected");

  const mc = await mariaPool.getConnection();
  mc.release();
  console.log("MariaDB connected");

  // 1. Vehicle registry — now, then every 30 min
  await safeVehicleSync();
  setInterval(safeVehicleSync, VEHICLE_INTERVAL);

  // 2. Full sync — starts 5 s after boot, every 30 s
  //    Loads device map (needed by quickSync) + telemetry history
  //    With bulk ops this now completes in ~3 s
  setTimeout(() => {
    console.log(`[Worker] Full sync every ${FULL_INTERVAL / 1000}s`);
    safeFullSync();
    setInterval(safeFullSync, FULL_INTERVAL);
  }, 5_000);

  // 3. Quick sync — starts 35 s after boot (device map must be loaded first)
  //    then every 10 s — keeps latest_positions fresh for live map
  setTimeout(() => {
    console.log(`[Worker] Quick sync every ${QUICK_INTERVAL / 1000}s`);
    safeQuickSync();
    setInterval(safeQuickSync, QUICK_INTERVAL);
  }, 35_000);
}

start().catch((e) => {
  console.error("[Worker] Fatal start error:", e.message);
  process.exit(1);
});