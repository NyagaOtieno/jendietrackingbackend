// src/workers/telemetryBufferWorker.js
import dotenv from "dotenv";
dotenv.config();

import { syncVehicles, runMariaSync, mariaPool } from "../services/mariaSync.service.js";
import { runLiveSync }                            from "../services/liveSync.service.js";
import { pgPool }                                from "../config/db.js";

// ─── Intervals ────────────────────────────────────────────────────────────────
//
//  LIVE_INTERVAL   — how often to sync the latest position for active devices.
//                    Default: 15 s.  Set LIVE_SYNC_INTERVAL in .env to override.
//                    This is what makes vehicles move on the map.
//
//  VEHICLE_INTERVAL — how often to sync the vehicle registry (plates, names).
//                    Default: 30 min.
//
//  TELEMETRY_INTERVAL — how often to run the full history sync from MariaDB.
//                    Default: 30 min.  Keep this long — it's expensive.
//
const LIVE_INTERVAL      = Number(process.env.LIVE_SYNC_INTERVAL  ||   15_000); //  15 s
const VEHICLE_INTERVAL   = Number(process.env.VEHICLE_SYNC_INTERVAL|| 1_800_000); // 30 min
const TELEMETRY_INTERVAL = Number(process.env.SYNC_INTERVAL        || 1_800_000); // 30 min

let liveBusy = false;
let telBusy  = false;
let vehBusy  = false;

async function liveTick() {
  if (liveBusy) return;
  liveBusy = true;
  try   { await runLiveSync(); }
  catch (e) { console.error("[Worker] Live sync error:", e.message); }
  finally   { liveBusy = false; }
}

async function vehicleTick() {
  if (vehBusy) return;
  vehBusy = true;
  try   { await syncVehicles(); }
  catch (e) { console.error("[Worker] Vehicle sync error:", e.message); }
  finally   { vehBusy = false; }
}

async function telemetryTick() {
  if (telBusy) return;
  telBusy = true;
  try   { await runMariaSync(); }
  catch (e) { console.error("[Worker] Telemetry sync error:", e.message); }
  finally   { telBusy = false; }
}

async function start() {
  // ── Verify connections ────────────────────────────────────────────────────
  const pg = await pgPool.connect();
  pg.release();
  console.log("✅ PostgreSQL connected");

  const mc = await mariaPool.getConnection();
  mc.release();
  console.log("MariaDB connected");

  // ── Vehicle registry (run once on boot, then every 30 min) ────────────────
  await vehicleTick();
  setInterval(vehicleTick, VEHICLE_INTERVAL);

  // ── Live position sync (starts 3 s after boot, runs every 15 s) ──────────
  setTimeout(async () => {
    console.log(`[Worker] Live sync every ${LIVE_INTERVAL / 1000}s`);
    await liveTick();
    setInterval(liveTick, LIVE_INTERVAL);
  }, 3_000);

  // ── Full telemetry history sync (starts 10 s after boot, runs every 30 min)
  setTimeout(async () => {
    console.log(`[Worker] Full telemetry sync every ${TELEMETRY_INTERVAL / 1000}s`);
    await telemetryTick();
    setInterval(telemetryTick, TELEMETRY_INTERVAL);
  }, 10_000);
}

start().catch((e) => {
  console.error("[Worker] Fatal:", e.message);
  process.exit(1);
});