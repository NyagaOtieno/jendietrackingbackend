import { pgPool } from '../config/db.js';
import { publishTelemetryBatch } from '../queue/publisher.js';

let isRunning = false;

const BATCH_SIZE = 1000;
const INTERVAL = 5000;
const MAX_RETRY = 5;

export async function startTelemetryBufferWorker() {
  console.log('🚀 Telemetry Buffer Worker started');

  setInterval(async () => {
    if (isRunning) {
      console.log('⏳ Worker still running, skipping cycle');
      return;
    }

    isRunning = true;

    try {
      const { rows } = await pgPool.query(`
        SELECT id, payload, retry_count
        FROM telemetry_ingestion_buffer
        WHERE status = 'PENDING'
        ORDER BY created_at ASC
        LIMIT $1
      `, [BATCH_SIZE]);

      if (rows.length === 0) {
        isRunning = false;
        return;
      }

      console.log(`📦 Processing ${rows.length} telemetry items`);

      const successfulIds = [];
      const failedUpdates = [];

      for (const row of rows) {
        try {
          const payload = JSON.parse(row.payload);

          await publishTelemetryBatch(payload);

          successfulIds.push(row.id);

        } catch (err) {
          const newRetry = (row.retry_count || 0) + 1;

          failedUpdates.push({
            id: row.id,
            retry: newRetry,
            status: newRetry > MAX_RETRY ? 'FAILED' : 'PENDING',
          });

          console.error(`❌ Telemetry failed id=${row.id}`, err.message);
        }
      }

      // ✅ Bulk update SUCCESS rows
      if (successfulIds.length) {
        await pgPool.query(`
          UPDATE telemetry_ingestion_buffer
          SET status='PROCESSED', processed_at=NOW()
          WHERE id = ANY($1)
        `, [successfulIds]);
      }

      // ✅ Bulk update FAILED/PENDING retries
      for (const f of failedUpdates) {
        await pgPool.query(`
          UPDATE telemetry_ingestion_buffer
          SET retry_count = $2,
              status = $3
          WHERE id = $1
        `, [f.id, f.retry, f.status]);
      }

    } catch (err) {
      console.error('🔥 Worker cycle error:', err.message);
    } finally {
      isRunning = false;
    }

  }, INTERVAL);
}