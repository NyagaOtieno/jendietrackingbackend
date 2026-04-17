import { pgPool } from '../config/db.js';
import { publishTelemetryBatch } from '../queue/publisher.js';

export async function startTelemetryBufferWorker() {
  console.log('🚀 Telemetry Buffer Worker started');

  setInterval(async () => {
    const { rows } = await pgPool.query(`
      SELECT *
      FROM telemetry_ingestion_buffer
      WHERE status = 'PENDING'
      ORDER BY created_at ASC
      LIMIT 1000
    `);

    if (!rows.length) return;

    for (const row of rows) {
      try {
        const payload = JSON.parse(row.payload);

        await publishTelemetryBatch(payload);

        await pgPool.query(`
          UPDATE telemetry_ingestion_buffer
          SET status='PROCESSED', processed_at=NOW()
          WHERE id=$1
        `, [row.id]);

      } catch (err) {
        await pgPool.query(`
          UPDATE telemetry_ingestion_buffer
          SET retry_count = retry_count + 1,
              status = CASE 
                WHEN retry_count > 5 THEN 'FAILED'
                ELSE 'PENDING'
              END
          WHERE id=$1
        `, [row.id]);
      }
    }
  }, 5000);
}