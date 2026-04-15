import pkg from 'pg';
const { Pool } = pkg;
const pool = new Pool({
  connectionString: 'postgresql://postgres:dDPbvTUlvzfxDpczOvbgzuMQdTNlvabl@crossover.proxy.rlwy.net:23922/railway',
  ssl: { rejectUnauthorized: false }
});
const res = await pool.query('SELECT table_name FROM information_schema.tables WHERE table_schema = $1', ['public']);
console.log('Tables:', res.rows.map(r => r.table_name));
await pool.end();
