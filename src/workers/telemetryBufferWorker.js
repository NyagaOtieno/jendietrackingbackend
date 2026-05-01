import { pgPool } from "../config/db.js";
import { publishTelemetryBatch } from "../queue/publisher.js";
import { runMariaSync } from "../services/mariaSync.service.js";
import { initMariaDB } from "../config/initDb.js";
import { fileURLToPath } from "url";
import path from "path";

// ─────────────────────────────────────────────
// INIT DB
// ─────────────────────────────────────────────
export async function initWorkerDependencies() {
  await initMariaDB();
}

// ─────────────────────────────────────────────
// STATE
// ─────────────────────────────────────────────
let isRunning = false;
let intervalRef = null;

const BATCH_SIZE = 1000;
const INTERVAL = 5000;
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
    console.error("MariaSync error:", e.message);
  } finally {
    mariaSyncRunning = false;
  }
}

// ─────────────────────────────────────────────
// WORKER START
// ─────────────────────────────────────────────
export function startTelemetryBufferWorker() {
  if (intervalRef) return;

  console.log("🚀 Telemetry Buffer Worker started");

  intervalRef = setInterval(processBatch, INTERVAL);
}

// ─────────────────────────────────────────────
// PROCESS BATCH (FIXED SAFE LOCK)
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

    if (!rows?.length) return;

    const successIds = [];

    for (const row of rows) {
      try {
        const payload = JSON.parse(row.payload);
        await publishTelemetryBatch(payload);
        successIds.push(row.id);
      } catch (e) {
        const retry = (row.retry_count || 0) + 1;

        await pgPool.query(`
          UPDATE telemetry_ingestion_buffer
          SET retry_count = $2,
              status = CASE WHEN $2 >= $3 THEN 'FAILED' ELSE 'PENDING' END
          WHERE id = $1
        `, [row.id, retry, MAX_RETRY]);
      }
    }

    if (successIds.length) {
      await pgPool.query(`
        UPDATE telemetry_ingestion_buffer
        SET status = 'PROCESSED',
            processed_at = NOW()
        WHERE id = ANY($1)
      `, [successIds]);
    }

  } catch (e) {
    console.error("Worker error:", e.message);
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

  safeMariaSync();
}

// ─────────────────────────────────────────────
// STOP
// ─────────────────────────────────────────────
export function stopTelemetryBufferWorker() {
  if (intervalRef) {
    clearInterval(intervalRef);
    intervalRef = null;
  }
}

// ─────────────────────────────────────────────
// AUTO START
// ─────────────────────────────────────────────
const isMainModule =
  process.argv[1] &&
  path.resolve(process.argv[1]) === path.resolve(fileURLToPath(import.meta.url));

if (isMainModule) {
  startSystem().catch(err => {
    console.error("Worker failed:", err);
    process.exit(1);
  });
}