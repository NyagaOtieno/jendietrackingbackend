import { redis } from "../config/redis.js";
import { pgPool } from "../config/db.js";
import { getIO } from "../socket/server.js";

export async function telemetryWorker() {
  console.log("🚀 Level 3 Worker started");

  while (true) {
    const data = await redis.xRead(
      { key: "telemetry:stream", id: "0" },
      { COUNT: 100, BLOCK: 5000 }
    );

    if (!data) continue;

    for (const stream of data) {
      for (const message of stream.messages) {
        const v = message.message;

        const deviceUid = v.deviceUid;

        const lat = Number(v.lat);
        const lon = Number(v.lon);

        // 1. WRITE HISTORY (batch later if needed)
        await pgPool.query(
          `INSERT INTO telemetry
           (device_uid, latitude, longitude, speed_kph, device_time)
           VALUES ($1,$2,$3,$4, NOW())`,
          [deviceUid, lat, lon, Number(v.speed)]
        );

        // 2. UPDATE LIVE STATE (CRITICAL OPTIMIZATION)
        await pgPool.query(
          `INSERT INTO latest_positions
           (device_uid, latitude, longitude, speed_kph, updated_at)
           VALUES ($1,$2,$3,$4,NOW())
           ON CONFLICT (device_uid) DO UPDATE SET
             latitude = EXCLUDED.latitude,
             longitude = EXCLUDED.longitude,
             speed_kph = EXCLUDED.speed_kph,
             updated_at = NOW()`,
          [deviceUid, lat, lon, Number(v.speed)]
        );

        // 3. CACHE (ELIMINATES DB READS)
        await redis.hSet(
          `latest:device:${deviceUid}`,
          {
            lat,
            lon,
            speed: v.speed,
            time: v.time
          }
        );

        // 4. SOCKET PUSH
        const io = getIO();
        io.emit("vehicle:update", {
          deviceUid,
          latitude: lat,
          longitude: lon,
          speedKph: Number(v.speed)
        });

        // 5. ALERT
        if (v.alarm) {
          io.emit("alert", {
            deviceUid,
            message: v.alarm,
            type: "alarm"
          });
        }
      }
    }
  }
}