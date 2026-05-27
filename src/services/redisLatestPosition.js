import { redis, redisPub } from "../config/redis.js";

const TTL_SECONDS = 60;

/**
 * SAFE BATCH UPSERT TO REDIS
 */
export async function setLatestPositionsBatch(positions = []) {
  if (!positions.length) return;

  try {
    const pipeline = redis.pipeline();

    for (const p of positions) {
  if (!p?.deviceId) continue;

  const key = `vehicle:${p.deviceId}:latest`;

  const lat = Number(p.lat ?? 0);
  const lng = Number(p.lng ?? p.lon ?? 0);

  const ts =
    p.dt instanceof Date && !isNaN(p.dt)
      ? p.dt.getTime()
      : Date.now();

  pipeline.hset(key, {
    lat,
    lng,
    speed: Number(p.speed ?? 0),
    heading: Number(p.heading ?? 0),
    timestamp: ts,
  });

  pipeline.expire(key, TTL_SECONDS);
}

    await pipeline.exec();

    try {
      await redisPub.publish(
        "vehicle_updates",
        JSON.stringify(positions)
      );
    } catch (pubErr) {
      console.error("Redis pub failed:", pubErr.message);
    }
  } catch (err) {
    console.error("Redis batch failed:", err.message);
  }
}

/**
 * GET SINGLE POSITION
 */
export async function getLatestPosition(deviceId) {
  if (!deviceId) return null;

  return await redis.hgetall(`vehicle:${deviceId}:latest`);
}

/**
 * BULK FETCH POSITIONS
 */
export async function getLatestPositionsBulk(deviceIds = []) {
  if (!deviceIds.length) return [];

  const pipeline = redis.pipeline();

  for (const id of deviceIds) {
    pipeline.hgetall(`vehicle:${id}:latest`);
  }

  const results = await pipeline.exec();

  return results.map((r, i) => {
    const data = r?.[1] || {};

    return {
      deviceId: deviceIds[i],
      lat: data.lat ? Number(data.lat) : null,
      lng: data.lng ? Number(data.lng) : null,
      speed: data.speed ? Number(data.speed) : 0,
      heading: data.heading ? Number(data.heading) : 0,
      timestamp: data.timestamp ? Number(data.timestamp) : null,
    };
  });
}