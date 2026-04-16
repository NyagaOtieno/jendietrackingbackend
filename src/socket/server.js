import { Server } from 'socket.io';

let io;

export function initWebSocket(server) {
  if (io) return io; // prevent double init

  io = new Server(server, {
    cors: { origin: '*' },
  });

  console.log('⚡ WebSocket initialized');

  return io;
}

export function getIO() {
  if (!io) {
    throw new Error('WebSocket not initialized');
  }
  return io;
}