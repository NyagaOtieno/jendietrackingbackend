import mariadb from "mariadb";
import dotenv from "dotenv";

dotenv.config();

export const mariaPool = mariadb.createPool({
  host: process.env.MARIA_HOST,
  port: Number(process.env.MARIA_PORT || 3306),
  user: process.env.MARIA_USER,
  password: process.env.MARIA_PASSWORD,
  database: process.env.MARIA_DATABASE,
  connectionLimit: 10,
  connectTimeout: 15000,
});