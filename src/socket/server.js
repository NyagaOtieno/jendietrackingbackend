import { Server } from "socket.io";
import { createAdapter } from "@socket.io/redis-adapter";

import { createClient } from "redis";

let io;

export async function initWebSocket(server) {
  if (io) return io;

  io = new Server(server, {
    cors: {
      origin: "*",
    },
    transports: ["websocket", "polling"],
  });

  // 🔥 IMPORTANT: DO NOT rely on external redisPub/redisSub here
  // Create proper socket.io compatible redis clients
  const pubClient = createClient({
    url: process.env.REDIS_URL,
  });

  const subClient = pubClient.duplicate();

  await pubClient.connect();
  await subClient.connect();

  io.adapter(createAdapter(pubClient, subClient));

  io.on("connection", (socket) => {
    console.log("Socket connected:", socket.id);

    socket.on("joinVehicle", (deviceId) => {
      socket.join(`vehicle:${deviceId}`);
    });

    socket.on("disconnect", () => {
      console.log("Socket disconnected:", socket.id);
    });
  });

  console.log("⚡ WebSocket + Redis adapter initialized");

  return io;
}

export function getIO() {
  return io;
}