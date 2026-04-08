const mysql = require("mysql2");

const connection = mysql.createConnection({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT || 3306,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  // force IPv4 if needed
  family: 4,
});

connection.connect((err) => {
  if (err) console.error("DB connection error:", err);
  else console.log("✅ Connected to MariaDB proxy!");
});