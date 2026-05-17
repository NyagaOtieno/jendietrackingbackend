// src/workers/telemetryBufferWorker.js
import dotenv from "dotenv";
dotenv.config();

import {
  syncVehicles,
  runMariaSync,
  runQuickSync,
  mariaPool,
} from "../services/mariaSync.service.js";
import { pgPool } from "../config/db.js";

// ── Intervals ─────────────────────────────────────────────────────────────────
//
//  QUICK_INTERVAL   — how often the lightweight "last 2 min only" sync runs.
//                     5 s = positions are at most 5 s stale in PostgreSQL.
//                     Combined with the frontend polling every 5 s, a GPS ping
//                     reaches the map within ~5-10 s end-to-end.
//
//  FULL_INTERVAL    — full MariaSync (device map + telemetry history).
//                     Kept at 30 s; the quick sync handles freshness.
//
//  VEHICLE_INTERVAL — vehicle registry (plates / names). 30 min is fine.
//
const QUICK_INTERVAL   = Number(process.env.LIVE_SYNC_INTERVAL    ||   5_000); //  5 s ← key change
const FULL_INTERVAL    = Number(process.env.SYNC_INTERVAL          ||  30_000); // 30 s
const VEHICLE_INTERVAL = Number(process.env.VEHICLE_SYNC_INTERVAL || 1_800_000); // 30 min

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

  // 1. Vehicle registry — now then every 30 min
  await safeVehicleSync();
  setInterval(safeVehicleSync, VEHICLE_INTERVAL);

  // 2. Full MariaSync — starts at 5 s, every 30 s
  //    Loads device map (required by quickSync) + writes telemetry history
  //    With bulk ops this completes in ~3 s
  setTimeout(() => {
    console.log(`[Worker] Full sync every ${FULL_INTERVAL / 1000}s`);
    safeFullSync();
    setInterval(safeFullSync, FULL_INTERVAL);
  }, 5_000);

  // 3. Quick sync — starts at 20 s (device map guaranteed loaded by then)
  //    runs every 5 s for near-real-time map updates
  //    Queries MariaDB for the last 2 min of activity only → ~1 s per run
  setTimeout(() => {
    console.log(`[Worker] Quick sync every ${QUICK_INTERVAL / 1000}s`);
    safeQuickSync();
    setInterval(safeQuickSync, QUICK_INTERVAL);
  }, 20_000);
}

start().catch((e) => {
  console.error("[Worker] Fatal:", e.message);
  process.exit(1);
});