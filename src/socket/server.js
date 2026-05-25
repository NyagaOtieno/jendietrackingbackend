import { Server } from "socket.io";
import { createAdapter } from "@socket.io/redis-adapter";

import {
  redisPub,
  redisSub
} from "../config/redis.js";

let io;

export function initWebSocket(server) {

  if (io) return io;

  io = new Server(server,{
    cors:{
      origin:"*"
    },

    transports:[
      "websocket",
      "polling"
    ]
  });

  io.adapter(
    createAdapter(
      redisPub,
      redisSub
    )
  );

  io.on("connection",(socket)=>{

    console.log(
      "Socket connected:",
      socket.id
    );

    socket.on(
      "joinVehicle",
      deviceId=>{
        socket.join(
          `vehicle:${deviceId}`
        );
      }
    );

    socket.on(
      "disconnect",
      ()=>{
        console.log(
          "Socket disconnected:",
          socket.id
        );
      }
    );

  });

  console.log(
    "⚡ WebSocket + Redis adapter initialized"
  );

  return io;
}

export function getIO(){
  return io;
}