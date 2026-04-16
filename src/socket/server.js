// src/socket/server.js
import { Server } from 'socket.io';

let io;

export function initWebSocket(server) {
  io = new Server(server, {
    cors: {
      origin: '*',
      methods: ['GET', 'POST'],
    },
  });

  io.on('connection', (socket) => {
    console.log('⚡ WS connected:', socket.id);

    socket.on('disconnect', () => {
      console.log('❌ WS disconnected:', socket.id);
    });
  });

  return io;
}

/**
 * IMPORTANT:
 * NO getIO export anymore (prevents your error)
 */