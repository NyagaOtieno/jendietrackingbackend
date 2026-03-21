import mysql from "mysql2/promise";
import dotenv from "dotenv";

dotenv.config();

export async function getMariaConnection() {
  return mysql.createConnection({
    host: process.env.MARIA_HOST,
    port: Number(process.env.MARIA_PORT || 3306),
    user: process.env.MARIA_USER,
    password: process.env.MARIA_PASSWORD,
    database: process.env.MARIA_DATABASE,
    connectTimeout: 15000,
    dateStrings: true,
    supportBigNumbers: true,
    bigNumberStrings: true,
    decimalNumbers: true,
  });
}