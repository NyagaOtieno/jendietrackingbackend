import { pgPool } from '../config/db.js';
import { publishTelemetryBatch } from '../queue/publisher.js';

let isRunning = false;
let intervalRef = null;

const BATCH_SIZE = 1000;
const INTERVAL = 5000;
const MAX_RETRY = 5;

export function startTelemetryBufferWorker() {
  console.log('🚀 Telemetry Buffer Worker started');

  // prevent double start (PM2 safety)
  if (intervalRef) {
    console.log('⚠️ Worker already running');
    return;
  }

  intervalRef = setInterval(processBatch, INTERVAL);
}

/**
 * =========================
 * MAIN PROCESS LOOP
 * =========================
 */
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

    if (!rows.length) {
      isRunning = false;
      return;
    }

    console.log(`📦 Processing ${rows.length} telemetry records`);

    const successIds = [];

    for (const row of rows) {
      try {
        const payload = JSON.parse(row.payload);

        await publishTelemetryBatch(payload);

        successIds.push(row.id);

      } catch (err) {
        const retry = (row.retry_count || 0) + 1;
        const status = retry >= MAX_RETRY ? 'FAILED' : 'PENDING';

        await pgPool.query(`
          UPDATE telemetry_ingestion_buffer
          SET retry_count = $2,
              status = $3
          WHERE id = $1
        `, [row.id, retry, status]);

        console.error(`❌ Failed id=${row.id}:`, err.message);
      }
    }

    // bulk success update (FAST)
    if (successIds.length > 0) {
      await pgPool.query(`
        UPDATE telemetry_ingestion_buffer
        SET status = 'PROCESSED',
            processed_at = NOW()
        WHERE id = ANY($1)
      `, [successIds]);
    }

  } catch (err) {
    console.error('🔥 Worker crash cycle:', err.message);
  } finally {
    isRunning = false;
  }
}

/**
 * =========================
 * GRACEFUL SHUTDOWN (IMPORTANT)
 * =========================
 */
export function stopTelemetryBufferWorker() {
  if (intervalRef) {
    clearInterval(intervalRef);
    intervalRef = null;
    console.log('🛑 Telemetry Worker stopped');
  }
}