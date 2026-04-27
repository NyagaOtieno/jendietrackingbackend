CREATE INDEX IF NOT EXISTS idx_latest_positions_vehicle_id
ON latest_positions(vehicle_id);

CREATE INDEX IF NOT EXISTS idx_latest_positions_device_uid
ON latest_positions(device_uid);

CREATE INDEX IF NOT EXISTS idx_vehicles_plate_number
ON vehicles(plate_number);

CREATE INDEX IF NOT EXISTS idx_users_email
ON users(email);