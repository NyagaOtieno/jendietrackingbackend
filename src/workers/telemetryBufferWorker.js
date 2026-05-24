import { pgPool } from "../config/db.js";
import { publishTelemetryBatch } from "../queue/publisher.js";
import { runMariaSync, syncVehicles } from "../services/mariaSync.service.js";
import { initMariaDB } from "../config/initDb.js";
import { fileURLToPath } from "url";
import path from "path";

// ─────────────────────────────────────────────
// INIT
// ─────────────────────────────────────────────
export async function initWorkerDependencies() {
  await initMariaDB();
}

// ─────────────────────────────────────────────
// STATE
// ─────────────────────────────────────────────
let isRunning = false;
let intervalRef = null;
let syncIntervalRef = null;
let vehicleSyncInterval = null;

const BATCH_SIZE = 1000;
const INTERVAL = 5000;
const SYNC_INTERVAL = 60_000;
const VEHICLE_SYNC_INTERVAL = 30 * 60_000;
const MAX_RETRY = 5;

let mariaSyncRunning = false;

// ─────────────────────────────────────────────
// SAFE MARIA SYNC
// ─────────────────────────────────────────────
async function safeMariaSync() {
  if (mariaSyncRunning) return;
  mariaSyncRunning = true;

  try {
    await runMariaSync();
  } catch (e) {
    console.error("[MariaSync Error]", e.message);
  } finally {
    mariaSyncRunning = false;
  }
}

async function safeVehicleSync() {
  try {
    await syncVehicles();
  } catch (e) {
    console.error("[VehicleSync Error]", e.message);
  }
}

// ─────────────────────────────────────────────
// WORKER START
// ─────────────────────────────────────────────
export function startTelemetryBufferWorker() {
  console.log("🚀 Telemetry Buffer Worker started");

  if (intervalRef) return;
  intervalRef = setInterval(processBatch, INTERVAL);
}

// ─────────────────────────────────────────────
// PROCESS BATCH
// ─────────────────────────────────────────────
async function processBatch() {
  if (isRunning) return;
  isRunning = true;

  try {
    const { rows } = await pgPool.query(`
      SELECT id, payload, retry_count
      FROM telemetry_ingestion_buffer
      WHERE status = 'PENDING'
      ORDER BY created_at ASC
      LIMIT $1
    `, [BATCH_SIZE]);

    console.log(`[Worker] Batch size: ${rows.length}`);

    const successIds = [];

    for (const row of rows) {
      try {
        const payload = JSON.parse(row.payload);
        await publishTelemetryBatch(payload);
        successIds.push(row.id);
      } catch (err) {
        const retry = (row.retry_count || 0) + 1;
        const status = retry >= MAX_RETRY ? "FAILED" : "PENDING";

        await pgPool.query(`
          UPDATE telemetry_ingestion_buffer
          SET retry_count = $2, status = $3
          WHERE id = $1
        `, [row.id, retry, status]);
      }
    }

    if (successIds.length) {
      await pgPool.query(`
        UPDATE telemetry_ingestion_buffer
        SET status = 'PROCESSED', processed_at = NOW()
        WHERE id = ANY($1)
      `, [successIds]);
    }
  } catch (e) {
    console.error("[Worker Error]", e.message);
  } finally {
    isRunning = false;
  }
}

// ─────────────────────────────────────────────
// START SYSTEM
// ─────────────────────────────────────────────
export async function startSystem() {
  console.log("🚀 Starting Telemetry System...");

  await initWorkerDependencies();

  startTelemetryBufferWorker();

  await safeMariaSync();
  syncIntervalRef = setInterval(safeMariaSync, SYNC_INTERVAL);

  vehicleSyncInterval = setInterval(safeVehicleSync, VEHICLE_SYNC_INTERVAL);

  console.log("[Worker] Sync active");
}

// ─────────────────────────────────────────────
// STOP
// ─────────────────────────────────────────────
export function stopTelemetryBufferWorker() {
  clearInterval(intervalRef);
  clearInterval(syncIntervalRef);
  clearInterval(vehicleSyncInterval);

  intervalRef = null;
  syncIntervalRef = null;
  vehicleSyncInterval = null;

  console.log("🛑 Worker stopped");
}

// ─────────────────────────────────────────────
// AUTO START
// ─────────────────────────────────────────────
const isMainModule =
  process.argv[1] &&
  path.resolve(process.argv[1]) === path.resolve(fileURLToPath(import.meta.url));

if (isMainModule) {
  startSystem().catch((err) => {
    console.error("Worker failed:", err);
    process.exit(1);
  });
}