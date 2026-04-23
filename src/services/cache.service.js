import { redis } from "../config/redis.js";

export async function cacheLatestPosition(deviceUid, data) {
  await redis.hset(`pos:${deviceUid}`, {
    lat: data.latitude,
    lon: data.longitude,
    speed: data.speedKph,
    heading: data.heading,
    time: data.deviceTime?.toISOString?.() || new Date().toISOString(),
  });

  await redis.sadd("active_devices", deviceUid);
}

export async function getAllCachedPositions() {
  const devices = await redis.smembers("active_devices");
  if (!devices.length) return [];

  const pipeline = redis.pipeline();
  devices.forEach(d => pipeline.hgetall(`pos:${d}`));

  const res = await pipeline.exec();

  return res.map(([_, data], i) => ({
    deviceUid: devices[i],
    ...data,
  }));
}