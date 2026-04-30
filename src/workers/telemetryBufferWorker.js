import { pgPool } from '../config/db.js';
import { publishTelemetryBatch } from '../queue/publisher.js';

let isRunning = false;
let intervalRef = null;

const BATCH_SIZE = 1000;
const INTERVAL   = 5000;
const MAX_RETRY  = 5;

export function startTelemetryBufferWorker() {
  if (intervalRef) {
    console.log('⚠️ Worker already running');
    return;
  }
  console.log('🚀 Telemetry Buffer Worker started');
  intervalRef = setInterval(processBatch, INTERVAL);
}

async function processBatch() {
  if (isRunning) return;
  isRunning = true;
  try {
    const { rows } = await pgPool.query(
      `SELECT id, payload, retry_count
       FROM telemetry_ingestion_buffer
       WHERE status = 'PENDING'
       ORDER BY created_at ASC LIMIT $1`,
      [BATCH_SIZE]
    );
    if (!rows.length) return;

    console.log(`📦 Processing ${rows.length} telemetry records`);
    const successIds = [];

    for (const row of rows) {
      try {
        await publishTelemetryBatch(JSON.parse(row.payload));
        successIds.push(row.id);
      } catch (err) {
        const retry  = (row.retry_count || 0) + 1;
        const status = retry >= MAX_RETRY ? 'FAILED' : 'PENDING';
        await pgPool.query(
          `UPDATE telemetry_ingestion_buffer SET retry_count=$2, status=$3 WHERE id=$1`,
          [row.id, retry, status]
        );
        console.error(`❌ Failed id=${row.id}:`, err.message);
      }
    }

    if (successIds.length > 0) {
      await pgPool.query(
        `UPDATE telemetry_ingestion_buffer SET status='PROCESSED', processed_at=NOW() WHERE id=ANY($1)`,
        [successIds]
      );
    }
  } catch (err) {
    console.error('🔥 Worker crash cycle:', err.message);
  } finally {
    isRunning = false;
  }
}

export function stopTelemetryBufferWorker() {
  if (intervalRef) {
    clearInterval(intervalRef);
    intervalRef = null;
    console.log('🛑 Telemetry Worker stopped');
  }
}
