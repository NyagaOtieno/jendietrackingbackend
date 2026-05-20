// src/workers/telemetryBufferWorker.js
import dotenv from "dotenv";
dotenv.config();

import { pgPool } from "../config/db.js";
import {
  syncVehicles, runQuickSync, runMariaSync,
  loadDeviceMap, initMariaSync, mariaPool,
} from "../services/mariaSync.service.js";

const QUICK_INTERVAL   = Number(process.env.LIVE_SYNC_INTERVAL    ||   5_000);
const FULL_INTERVAL    = Number(process.env.SYNC_INTERVAL          ||  60_000);
const VEHICLE_INTERVAL = Number(process.env.VEHICLE_SYNC_INTERVAL || 1_800_000);

let quickBusy=false, fullBusy=false, vehicleBusy=false;

async function safeQuickSync() {
  if (quickBusy) return; quickBusy=true;
  try   { await runQuickSync(); }
  catch (e) { console.error("[Worker] quickSync error:", e.message); }
  finally   { quickBusy=false; }
}

async function safeFullSync() {
  if (fullBusy) return; fullBusy=true;
  try   { await runMariaSync(); }
  catch (e) { console.error("[Worker] fullSync error:", e.message); }
  finally   { fullBusy=false; }
}

async function safeVehicleSync() {
  if (vehicleBusy) return; vehicleBusy=true;
  try   { await syncVehicles(); await loadDeviceMap(); }
  catch (e) { console.error("[Worker] vehicleSync error:", e.message); }
  finally   { vehicleBusy=false; }
}

process.on("uncaughtException",  e => console.error("[Worker] Uncaught:", e.message));
process.on("unhandledRejection", e => console.error("[Worker] Unhandled:", e));

async function start() {
  const pg = await pgPool.connect(); pg.release();
  console.log("✅ PostgreSQL connected");
  const mc = await mariaPool.getConnection(); mc.release();
  console.log("✅ MariaDB connected");

  // Restore checkpoint + device map from PG — fast, no MariaDB needed
  await initMariaSync();

  // quickSync: starts immediately with restored checkpoint
  console.log(`[Worker] ⚡ quickSync every ${QUICK_INTERVAL/1000}s`);
  await safeQuickSync();
  setInterval(safeQuickSync, QUICK_INTERVAL);

  // vehicleSync: batched bulk, runs 10s after start so quickSync gets priority
  setTimeout(() => {
    console.log(`[Worker] 🚗 vehicleSync every ${VEHICLE_INTERVAL/60000}min`);
    safeVehicleSync();
    setInterval(safeVehicleSync, VEHICLE_INTERVAL);
  }, 10_000);

  // fullSync: history telemetry, 30s after start
  setTimeout(() => {
    console.log(`[Worker] 📦 fullSync every ${FULL_INTERVAL/1000}s`);
    safeFullSync();
    setInterval(safeFullSync, FULL_INTERVAL);
  }, 30_000);
}

start().catch(e => { console.error("[Worker] Fatal:", e.message); process.exit(1); });