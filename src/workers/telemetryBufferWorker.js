import { pgPool } from "../config/db.js";
import { publishTelemetryBatch } from "../queue/publisher.js";

let isRunning = false;
let intervalRef = null;

const BATCH_SIZE = 1000;
const INTERVAL = 5000;
const MAX_RETRY = 5;

export function startTelemetryBufferWorker() {
  console.log("🚀 Telemetry Worker started");

  if (intervalRef) return;

  intervalRef = setInterval(processBatch, INTERVAL);
}

async function processBatch() {
  if (isRunning) return;

  isRunning = true;

  try {
    const { rows } = await pgPool.query(
      `
      SELECT id, payload, retry_count
      FROM telemetry_ingestion_buffer
      WHERE status = 'PENDING'
      ORDER BY created_at ASC
      LIMIT $1
      `,
      [BATCH_SIZE]
    );

    if (!rows.length) return;

    const success = [];

    for (const r of rows) {
      try {
        await publishTelemetryBatch(JSON.parse(r.payload));
        success.push(r.id);
      } catch (err) {
        const retry = (r.retry_count || 0) + 1;

        await pgPool.query(
          `
          UPDATE telemetry_ingestion_buffer
          SET retry_count=$2,
              status = CASE WHEN $2 >= $3 THEN 'FAILED' ELSE 'PENDING' END
          WHERE id=$1
          `,
          [r.id, retry, MAX_RETRY]
        );
      }
    }

    if (success.length) {
      await pgPool.query(
        `
        UPDATE telemetry_ingestion_buffer
        SET status='PROCESSED', processed_at=NOW()
        WHERE id = ANY($1)
        `,
        [success]
      );
    }
  } catch (e) {
    console.error("Worker error:", e.message);
  } finally {
    isRunning = false;
  }
}

export function stopTelemetryBufferWorker() {
  if (intervalRef) clearInterval(intervalRef);
  intervalRef = null;
}