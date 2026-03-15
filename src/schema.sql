CREATE TABLE IF NOT EXISTS users (
  id SERIAL PRIMARY KEY,
  full_name VARCHAR(150) NOT NULL,
  email VARCHAR(150) NOT NULL UNIQUE,
  phone VARCHAR(30),
  password_hash TEXT NOT NULL,
  role VARCHAR(50) NOT NULL DEFAULT 'staff',
  status VARCHAR(20) NOT NULL DEFAULT 'active',
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS accounts (
  id SERIAL PRIMARY KEY,
  account_type VARCHAR(30) NOT NULL DEFAULT 'individual',
  account_name VARCHAR(150) NOT NULL,
  client_code VARCHAR(50),
  company_code VARCHAR(50),
  sacco_code VARCHAR(50),
  status VARCHAR(20) NOT NULL DEFAULT 'active',
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS vehicles (
  id SERIAL PRIMARY KEY,
  plate_number VARCHAR(30) NOT NULL UNIQUE,
  unit_name VARCHAR(150),
  make VARCHAR(100),
  model VARCHAR(100),
  year INTEGER,
  account_id INTEGER REFERENCES accounts(id) ON DELETE SET NULL,
  status VARCHAR(20) NOT NULL DEFAULT 'active',
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS devices (
  id SERIAL PRIMARY KEY,
  device_uid VARCHAR(100) NOT NULL UNIQUE,
  label VARCHAR(150),
  imei VARCHAR(50) UNIQUE,
  sim_number VARCHAR(50),
  protocol_type VARCHAR(50),
  vehicle_id INTEGER UNIQUE REFERENCES vehicles(id) ON DELETE SET NULL,
  expires_at TIMESTAMP NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS telemetry (
  id BIGSERIAL PRIMARY KEY,
  device_id INTEGER NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
  latitude NUMERIC(10, 7) NOT NULL,
  longitude NUMERIC(10, 7) NOT NULL,
  speed_kph NUMERIC(10, 2),
  heading NUMERIC(10, 2),
  device_time TIMESTAMP NULL,
  received_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS latest_positions (
  device_id INTEGER PRIMARY KEY REFERENCES devices(id) ON DELETE CASCADE,
  latitude NUMERIC(10, 7) NOT NULL,
  longitude NUMERIC(10, 7) NOT NULL,
  speed_kph NUMERIC(10, 2),
  heading NUMERIC(10, 2),
  device_time TIMESTAMP NULL,
  received_at TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_telemetry_device_received_at
ON telemetry(device_id, received_at DESC);

CREATE INDEX IF NOT EXISTS idx_devices_vehicle_id
ON devices(vehicle_id);

CREATE INDEX IF NOT EXISTS idx_vehicles_account_id
ON vehicles(account_id);