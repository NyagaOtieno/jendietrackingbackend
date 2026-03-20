import mysql from "mysql2/promise";
import dotenv from "dotenv";

dotenv.config();

const mariadbPool = mysql.createPool({
  host: process.env.MARIA_HOST,
  port: Number(process.env.MARIA_PORT || 3306),
  user: process.env.MARIA_USER,
  password: process.env.MARIA_PASSWORD,
  database: process.env.MARIA_DATABASE,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
  connectTimeout: 15000,
});

export default mariadbPool;