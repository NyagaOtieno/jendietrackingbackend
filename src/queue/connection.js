let channel = null;

export async function connect() {
  console.log("[RabbitMQ] Safe mode enabled (no connection)");
  return null;
}

export function getChannel() {
  return null;
}

export async function connectRabbitMQ() {
  console.log("[RabbitMQ] Disabled safely");
  return null;
}

export default { connect, getChannel, connectRabbitMQ };