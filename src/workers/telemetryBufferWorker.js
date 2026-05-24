import { pgPool } from "../config/db.js";
import { publishTelemetryBatch } from "../queue/publisher.js";
import { runMariaSync, syncVehicles } from "../services/mariaSync.service.js";

let isRunning = false;

const INTERVAL = 5000;
const SYNC_INTERVAL = 60000;
const VEHICLE_INTERVAL = 1800000;

let batchTimer;
let syncTimer;
let vehicleTimer;

const MAX_RETRY = 5;

export function startTelemetryBufferWorker() {
  console.log("🚀 Telemetry Buffer Worker started");

  if (batchTimer) return;

  batchTimer = setInterval(processBatch, INTERVAL);

  // Maria sync loop
  syncTimer = setInterval(async () => {
    await runMariaSync();
  }, SYNC_INTERVAL);

  // vehicle sync loop
  vehicleTimer = setInterval(async () => {
    await syncVehicles();
  }, VEHICLE_INTERVAL);
}

async function processBatch() {
  if (isRunning) return;
  isRunning = true;

  try {
    const { rows } = await pgPool.query(`
      SELECT id, payload, retry_count
      FROM telemetry_ingestion_buffer
      WHERE status = 'PENDING'
      ORDER BY created_at ASC
      LIMIT 1000
    `);

    const success = [];

    for (const row of rows) {
      try {
        const payload = JSON.parse(row.payload);
        await publishTelemetryBatch(payload);
        success.push(row.id);
      } catch (e) {
        const retry = (row.retry_count || 0) + 1;
        const status = retry >= MAX_RETRY ? "FAILED" : "PENDING";

        await pgPool.query(`
          UPDATE telemetry_ingestion_buffer
          SET retry_count = $2,
              status = $3
          WHERE id = $1
        `, [row.id, retry, status]);
      }
    }

    if (success.length) {
      await pgPool.query(`
        UPDATE telemetry_ingestion_buffer
        SET status = 'PROCESSED',
            processed_at = NOW()
        WHERE id = ANY($1)
      `, [success]);
    }

  } catch (e) {
    console.error("[Worker Error]", e.message);
  } finally {
    isRunning = false;
  }
}

export function stopTelemetryBufferWorker() {
  clearInterval(batchTimer);
  clearInterval(syncTimer);
  clearInterval(vehicleTimer);

  batchTimer = null;
  syncTimer = null;
  vehicleTimer = null;

  console.log("🛑 Worker stopped");
}