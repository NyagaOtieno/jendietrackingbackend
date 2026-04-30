import { pgPool } from "../config/db.js";
import { getLatestPositionsBulk } from "../services/redisLatestPosition.js";

export async function getLatestPositions(req, res) {
  const { rows } = await pgPool.query("SELECT id FROM devices LIMIT 1000");

  const ids = rows.map(r => r.id);

  const data = await getLatestPositionsBulk(ids);

  res.json(data);
}