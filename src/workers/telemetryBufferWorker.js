import dotenv from 'dotenv';
dotenv.config();

import { pgPool } from '../config/db.js';

// =========================
// CONFIG
// =========================
const BATCH_SIZE = Number(process.env.BUFFER_BATCH_SIZE || 500);
const MAX_RETRIES = 5;
const SLEEP_MS = 2000;

// =========================
// SLEEP
// =========================
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// =========================
// PROCESS BATCH
// =========================
async function processBatch(rows) {
  if (!rows.length) return;

  const values = [];
  const placeholders = [];

  let i = 1;

  for (const row of rows) {
    const payload = row.payload;

    placeholders.push(
      `($${i++}, $${i++}, $${i++}, $${i++}, $${i++}, $${i++})`
    );

    values.push(
      row.device_id,
      payload.receivedAt || new Date(),
      payload.latitude,
      payload.longitude,
      payload.speedKph,
      payload.heading
    );
  }

  const query = `
    INSERT INTO telemetry (
      device_id,
      recorded_at,
      latitude,
      longitude,
      speed,
      heading
    )
    VALUES ${placeholders.join(',')}
    ON CONFLICT (device_id, recorded_at)
    DO NOTHING
  `;

  await pgPool.query(query, values);
}

// =========================
// MAIN LOOP
// =========================
export async function startTelemetryBufferWorker() {
  console.log('🚀 Telemetry Buffer Worker started');

  while (true) {
    try {
      const { rows } = await pgPool.query(
        `
        SELECT *
        FROM telemetry_ingestion_buffer
        WHERE status = 'PENDING'
        ORDER BY id ASC
        LIMIT $1
        FOR UPDATE SKIP LOCKED
        `,
        [BATCH_SIZE]
      );

      if (!rows.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      try {
        await processBatch(rows);

        // ✅ MARK SUCCESS
        const ids = rows.map(r => r.id);

        await pgPool.query(
          `
          UPDATE telemetry_ingestion_buffer
          SET status = 'PROCESSED',
              processed_at = NOW()
          WHERE id = ANY($1)
          `,
          [ids]
        );

        console.log(`✅ Processed batch: ${rows.length}`);

      } catch (err) {
        console.error('❌ Batch failed:', err.message);

        // 🔁 RETRY HANDLING
        for (const row of rows) {
          if (row.retry_count >= MAX_RETRIES) {
            await pgPool.query(
              `
              UPDATE telemetry_ingestion_buffer
              SET status = 'FAILED',
                  retry_count = retry_count + 1
              WHERE id = $1
              `,
              [row.id]
            );
          } else {
            await pgPool.query(
              `
              UPDATE telemetry_ingestion_buffer
              SET retry_count = retry_count + 1
              WHERE id = $1
              `,
              [row.id]
            );
          }
        }
      }

    } catch (err) {
      console.error('❌ Worker loop error:', err.message);
      await sleep(3000);
    }
  }
}