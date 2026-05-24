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

let mariaSyncRunning = false;
let consecutiveFailures = 0;
let lastHeartbeat = Date.now();

const BATCH_SIZE = 1000;
const INTERVAL = 5000;
const SYNC_INTERVAL = 60_000;
const VEHICLE_SYNC_INTERVAL = 30 * 60_000;
const MAX_RETRY = 5;

// ─────────────────────────────────────────────
// HEARTBEAT
// ─────────────────────────────────────────────
setInterval(() => {
  lastHeartbeat = Date.now();
  console.log(`[Worker] heartbeat ${new Date().toISOString()}`);
}, 10000);

// ─────────────────────────────────────────────
// SAFE MARIA SYNC
// ─────────────────────────────────────────────
async function safeMariaSync() {
  if (mariaSyncRunning) return;

  mariaSyncRunning = true;

  try {
    consecutiveFailures = 0;
    await runMariaSync();
  } catch (e) {
    consecutiveFailures++;

    console.error("[MariaSync Error]", {
      message: e.message,
      failures: consecutiveFailures,
    });

    if (consecutiveFailures >= 5) {
      console.error("[MariaSync] Cooling down...");
      await new Promise((r) => setTimeout(r, 30000));
      consecutiveFailures = 0;
    }
  } finally {
    mariaSyncRunning = false;
  }
}

// ─────────────────────────────────────────────
// VEHICLE SYNC
// ─────────────────────────────────────────────
async function safeVehicleSync() {
  try {
    await syncVehicles();
  } catch (e) {
    console.error("[VehicleSync Error]", e.message);
  }
}

// ─────────────────────────────────────────────
// BUFFER WORKER START
// ─────────────────────────────────────────────
export function startTelemetryBufferWorker() {
  console.log("🚀 Telemetry Buffer Worker started");

  if (intervalRef) return;

  intervalRef = setInterval(processBatch, INTERVAL);
}

// ─────────────────────────────────────────────
// PROCESS BATCH (SAFE + ATOMIC)
// ─────────────────────────────────────────────
async function processBatch() {
  if (isRunning) return;
  isRunning = true;

  const client = await pgPool.connect();

  try {
    const lock = await client.query(
      `SELECT pg_try_advisory_lock(123456) AS locked`
    );

    if (!lock.rows[0].locked) {
      client.release();
      isRunning = false;
      return;
    }

    await client.query("BEGIN");

    const { rows } = await client.query(
      `
      SELECT id, payload, retry_count
      FROM telemetry_ingestion_buffer
      WHERE status = 'PENDING'
      ORDER BY created_at ASC
      LIMIT $1
      `,
      [BATCH_SIZE]
    );

    console.log(`[Worker] Batch size: ${rows.length}`);

    const successIds = [];

    for (const row of rows) {
      let payload;

      try {
        payload = JSON.parse(row.payload);
      } catch (e) {
        await client.query(
          `UPDATE telemetry_ingestion_buffer SET status='FAILED' WHERE id=$1`,
          [row.id]
        );
        continue;
      }

      try {
        await publishTelemetryBatch(payload);
        successIds.push(row.id);
      } catch (err) {
        const retry = (row.retry_count || 0) + 1;
        const status = retry >= MAX_RETRY ? "FAILED" : "PENDING";

        await client.query(
          `
          UPDATE telemetry_ingestion_buffer
          SET retry_count=$2, status=$3
          WHERE id=$1
          `,
          [row.id, retry, status]
        );
      }
    }

    if (successIds.length) {
      await client.query(
        `
        UPDATE telemetry_ingestion_buffer
        SET status='PROCESSED', processed_at=NOW()
        WHERE id = ANY($1)
        `,
        [successIds]
      );
    }

    await client.query("COMMIT");

    await client.query(`SELECT pg_advisory_unlock(123456)`);
  } catch (e) {
    await client.query("ROLLBACK");
    console.error("[Worker Error]", e.message);
  } finally {
    client.release();
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

  setTimeout(async () => {
    await safeMariaSync();
    syncIntervalRef = setInterval(safeMariaSync, SYNC_INTERVAL);
  }, 5000);

  vehicleSyncInterval = setInterval(
    safeVehicleSync,
    VEHICLE_SYNC_INTERVAL
  );

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
  path.resolve(process.argv[1]) ===
    path.resolve(fileURLToPath(import.meta.url));

if (isMainModule) {
  startSystem().catch((err) => {
    console.error("Worker failed:", err);
    process.exit(1);
  });
}