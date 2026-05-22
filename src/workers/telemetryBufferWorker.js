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

const QUICK_INTERVAL   = Number(process.env.LIVE_SYNC_INTERVAL    || 4000);   // 4s
const FULL_INTERVAL    = Number(process.env.SYNC_INTERVAL          || 30000);  // 30s
const VEHICLE_INTERVAL = Number(process.env.VEHICLE_SYNC_INTERVAL || 1800000); // 30 min

let quickBusy = false;
let fullBusy = false;
let vehBusy = false;

/* ---------------- SAFE WRAPPERS ---------------- */

async function safeQuickSync() {
  if (quickBusy) return;
  quickBusy = true;

  try {
    await runQuickSync();
  } catch (e) {
    console.error("[Worker] quickSync error:", e.message);
  } finally {
    quickBusy = false;
  }
}

async function safeFullSync() {
  if (fullBusy) return;
  fullBusy = true;

  try {
    await runMariaSync();
  } catch (e) {
    console.error("[Worker] MariaSync error:", e.message);
  } finally {
    fullBusy = false;
  }
}

async function safeVehicleSync() {
  if (vehBusy) return;
  vehBusy = true;

  try {
    await syncVehicles();
  } catch (e) {
    console.error("[Worker] Vehicle sync error:", e.message);
  } finally {
    vehBusy = false;
  }
}

/* ---------------- LOOP ENGINE (NO setInterval) ---------------- */

async function loopQuickSync() {
  while (true) {
    const start = Date.now();

    await safeQuickSync();

    const elapsed = Date.now() - start;
    const wait = Math.max(0, QUICK_INTERVAL - elapsed);

    await new Promise((res) => setTimeout(res, wait));
  }
}

async function loopFullSync() {
  while (true) {
    const start = Date.now();

    await safeFullSync();

    const elapsed = Date.now() - start;
    const wait = Math.max(0, FULL_INTERVAL - elapsed);

    await new Promise((res) => setTimeout(res, wait));
  }
}

function startHeartbeat() {
  setInterval(() => {
    console.log("🟢 Worker heartbeat OK");
  }, 10000);
}

/* ---------------- START ---------------- */

async function start() {
  // DB checks
  const pg = await pgPool.connect();
  pg.release();
  console.log("✅ PostgreSQL connected");

  const mc = await mariaPool.getConnection();
  mc.release();
  console.log("MariaDB connected");

  // 1. Vehicle sync
  await safeVehicleSync();
  setInterval(safeVehicleSync, VEHICLE_INTERVAL);

  // 2. Start full sync after 5s
  setTimeout(() => {
    console.log(`[Worker] Full sync every ${FULL_INTERVAL / 1000}s`);
    loopFullSync();
  }, 5000);

  // 3. Start quick sync after 20s
  setTimeout(() => {
    console.log(`[Worker] Quick sync every ${QUICK_INTERVAL / 1000}s`);
    loopQuickSync();
  }, 20000);

  // heartbeat
  startHeartbeat();
}

/* ---------------- ERROR HANDLING ---------------- */

process.on("uncaughtException", (e) => {
  console.error("[Worker] Uncaught:", e.message);
});

process.on("unhandledRejection", (e) => {
  console.error("[Worker] Unhandled:", e);
});

/* ---------------- BOOT ---------------- */

start().catch((e) => {
  console.error("[Worker] Fatal:", e.message);
  process.exit(1);
});