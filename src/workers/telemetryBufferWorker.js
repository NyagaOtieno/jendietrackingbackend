import { pgPool } from "../config/db.js";
import { publishTelemetryBatch } from "../queue/publisher.js";

let isRunning = false;
let intervalRef = null;

const BATCH_SIZE = 1000;
const INTERVAL = 5000;
const MAX_RETRY = 5;

export function startTelemetryBufferWorker() {
  console.log("🚀 Telemetry Buffer Worker started");

  if (intervalRef) return;

  intervalRef = setInterval(processBatch, INTERVAL);
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
      LIMIT $1
    `, [BATCH_SIZE]);

    if (!rows.length) return;

    const successIds = [];

    for (const row of rows) {
      try {
        await publishTelemetryBatch(JSON.parse(row.payload));
        successIds.push(row.id);
      } catch (err) {
        const retry = (row.retry_count || 0) + 1;

        await pgPool.query(
          `
          UPDATE telemetry_ingestion_buffer
          SET retry_count=$2,
              status=$3
          WHERE id=$1
          `,
          [row.id, retry, retry >= MAX_RETRY ? "FAILED" : "PENDING"]
        );
      }
    }

    if (successIds.length) {
      await pgPool.query(
        `
        UPDATE telemetry_ingestion_buffer
        SET status='PROCESSED',
            processed_at=NOW()
        WHERE id = ANY($1)
        `,
        [successIds]
      );
    }
  } catch (e) {
    console.error("Worker error:", e.message);
  } finally {
    isRunning = false;
  }
}

export function stopTelemetryBufferWorker() {
  if (intervalRef) {
    clearInterval(intervalRef);
    intervalRef = null;
  }
}