import { Server } from 'socket.io';

let io = null;

// =========================
// INIT WEBSOCKET
// =========================
export function initWebSocket(server) {
  io = new Server(server, {
    cors: {
      origin: '*',
      methods: ['GET', 'POST'],
    },
  });

  io.on('connection', (socket) => {
    console.log('⚡ client connected:', socket.id);

    socket.on('disconnect', () => {
      console.log('❌ client disconnected:', socket.id);
    });
  });

  return io;
}

// =========================
// GET IO INSTANCE (THIS FIXES YOUR ERROR)
// =========================
export function getIO() {
  if (!io) {
    throw new Error('Socket not initialized. Call initWebSocket(server) first.');
  }
  return io;
}