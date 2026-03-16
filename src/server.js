
import dotenv from "dotenv";
import express from "express";
import cors from "cors";

import { testDbConnection } from "./config/db.js";
import positionsRoutes from "./routes/positions.routes.js";
import fleetRoutes from "./routes/fleet.routes.js";
import authRoutes from "./routes/auth.routes.js";
import seedRoutes from "./routes/seed.routes.js";
import devicesRoutes from "./routes/devices.routes.js";
import accountsRoutes from "./routes/accounts.routes.js";

dotenv.config();

const app = express();

app.use(
  cors({
    origin: process.env.FRONTEND_ORIGIN?.split(",").map((s) => s.trim()) || "*",
    credentials: true,
  })
);

app.use(express.json());

app.get("/health", async (_req, res) => {
  let db = "down";

  try {
    await testDbConnection();
    db = "up";
  } catch {
    db = "down";
  }

  res.json({
    success: true,
    message: "Backend is running",
    database: db,
  });
});

app.use("/v1/auth", authRoutes);
app.use("/v1/seed", seedRoutes);
app.use("/v1/accounts", accountsRoutes);
app.use("/v1/devices", devicesRoutes);
app.use("/v1/positions", positionsRoutes);
app.use("/v1/fleet", fleetRoutes);

app.use((req, res) => {
  res.status(404).json({
    success: false,
    message: `Route not found: ${req.method} ${req.originalUrl}`,
  });
});

app.use((error, _req, res, _next) => {
  console.error("Unhandled server error:", error);
  res.status(500).json({
    success: false,
    message: "Internal server error",
  });
});

const PORT = process.env.PORT || 4000;

app.listen(PORT, () => {
  console.log(`Backend running on http://localhost:${PORT}`);
});
