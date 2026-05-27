// src/workers/telemetryBufferWorker.js
import dotenv from "dotenv";
dotenv.config();

import { pgPool } from "../config/db.js";
import {
  syncVehicles,
  runLiveSync,
  runMariaSync,
  loadDeviceMap,
  initMariaSync,
  mariaPool,
} from "../services/mariaSync.service.js";

// ── Intervals ──────────────────────────────────────────────────────────────
const LIVE_INTERVAL    = Number(process.env.LIVE_SYNC_INTERVAL    ||   1_000); // 1s  ← live positions
const FULL_INTERVAL    = Number(process.env.SYNC_INTERVAL          ||  60_000); // 60s ← telemetry history
const VEHICLE_INTERVAL = Number(process.env.VEHICLE_SYNC_INTERVAL || 1_800_000); // 30m ← plates/names

let liveBusy    = false;
let fullBusy    = false;
let vehicleBusy = false;

// ── Guards ─────────────────────────────────────────────────────────────────
async function safeLiveSync() {
  if (liveBusy) return;
  liveBusy = true;
  try   { await runLiveSync(); }
  catch (e) { console.error("[Worker] liveSync error:", e.message); }
  finally   { liveBusy = false; }
}

async function safeFullSync() {
  if (fullBusy) return;
  fullBusy = true;
  try   { await runMariaSync(); }
  catch (e) { console.error("[Worker] fullSync error:", e.message); }
  finally   { fullBusy = false; }
}

async function safeVehicleSync() {
  if (vehicleBusy) return;
  vehicleBusy = true;
  try   { await syncVehicles(); await loadDeviceMap(); }
  catch (e) { console.error("[Worker] vehicleSync error:", e.message); }
  finally   { vehicleBusy = false; }
}

// ── Crash safety ───────────────────────────────────────────────────────────
process.on("uncaughtException",  e => console.error("[Worker] Uncaught:", e.message));
process.on("unhandledRejection", e => console.error("[Worker] Unhandled:", String(e)));

// ── Startup ────────────────────────────────────────────────────────────────
async function start() {
  // Health checks
  const pg = await pgPool.connect(); pg.release();
  console.log("✅ PostgreSQL connected");
  const mc = await mariaPool.getConnection(); mc.release();
  console.log("✅ MariaDB connected");

  // Init: restore checkpoint from PG + load device map
  // This runs BEFORE any sync so liveSync starts with correct lastEventId
  // meaning NO 10-15 min backlog on restart
  await initMariaSync();

  // ── 1. LIVE SYNC — every 1 second, starts immediately ─────────────────
  // Uses persistent MariaDB connection + PK range query = <10ms per tick
  // Writes directly to latest_positions with updated_at = NOW()
  console.log(`[Worker] ⚡ liveSync every ${LIVE_INTERVAL}ms`);
  await safeLiveSync();
  setInterval(safeLiveSync, LIVE_INTERVAL);

  // ── 2. VEHICLE SYNC — every 30 min, starts after 15s ─────────────────
  // Batched bulk upserts (50 queries, not 5000) so PG never crashes
  setTimeout(() => {
    console.log(`[Worker] 🚗 vehicleSync every ${VEHICLE_INTERVAL / 60000}min`);
    safeVehicleSync();
    setInterval(safeVehicleSync, VEHICLE_INTERVAL);
  }, 15_000);

  // ── 3. FULL SYNC — every 60s, starts after 30s ────────────────────────
  // Writes telemetry history table, liveSync handles freshness
  setTimeout(() => {
    console.log(`[Worker] 📦 fullSync every ${FULL_INTERVAL / 1000}s`);
    safeFullSync();
    setInterval(safeFullSync, FULL_INTERVAL);
  }, 30_000);
}

start().catch(e => {
  console.error("[Worker] Fatal startup error:", e.message);
  process.exit(1);
});