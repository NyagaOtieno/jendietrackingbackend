import mariadb from "mariadb";
import dotenv from "dotenv";

dotenv.config();

export const mariaPool = mariadb.createPool({
  host: process.env.MARIA_DB_HOST,
  user: process.env.MARIA_DB_USER,
  password: process.env.MARIA_DB_PASSWORD,
  database: process.env.MARIA_DB_NAME,
  connectionLimit: 10,
  acquireTimeout: 10000,
});