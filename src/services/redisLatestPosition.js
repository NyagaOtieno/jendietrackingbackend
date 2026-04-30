// services/redisLatestPosition.js
import { redis } from "../config/redisClient.js";

const TTL_SECONDS = 60;

// Single update (fallback)
export async function setLatestPosition(vehicleId, data) {
  const key = `vehicle:${vehicleId}:latest`;

  await redis.hSet(key, {
    lat: data.lat,
    lng: data.lng,
    speed: data.speed,
    heading: data.heading,
    timestamp: data.timestamp,
  });

  await redis.expire(key, TTL_SECONDS);
}

// 🔥 Batch update (USE THIS in mariasync)
export async function setLatestPositionsBatch(positions) {
  const pipeline = redis.multi();

  for (const p of positions) {
    const key = `vehicle:${p.deviceId}:latest`;

    pipeline.hSet(key, {
      lat: p.lat,
      lng: p.lon,
      speed: p.speed,
      heading: p.heading,
      timestamp: p.dt.getTime(),
    });

    pipeline.expire(key, TTL_SECONDS);

    // optional realtime push
    pipeline.publish("vehicle_updates", JSON.stringify({
      deviceId: p.deviceId,
      lat: p.lat,
      lng: p.lon,
      speed: p.speed
    }));
  }

  await pipeline.exec();
}

// Fetch one
export async function getLatestPosition(vehicleId) {
  return redis.hGetAll(`vehicle:${vehicleId}:latest`);
}

// Fetch many (FAST)
export async function getLatestPositionsBulk(deviceIds) {
  const pipeline = redis.multi();

  deviceIds.forEach(id => {
    pipeline.hGetAll(`vehicle:${id}:latest`);
  });

  const results = await pipeline.exec();

  return results.map((r, i) => ({
    deviceId: deviceIds[i],
    ...r[1]
  }));
}