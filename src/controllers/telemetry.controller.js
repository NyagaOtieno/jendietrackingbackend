export const ingestTelemetry = async (req, res) => {
  try {
    const { latitude, longitude, speed, ignition, recordedAt } = req.body;

    const deviceId = req.device.id;

    if (!latitude || !longitude) {
      return res.status(400).json({
        success: false,
        message: "latitude and longitude are required",
      });
    }

    await query(
      `
      INSERT INTO telemetry (
        device_id,
        latitude,
        longitude,
        speed,
        ignition,
        recorded_at
      )
      VALUES ($1, $2, $3, $4, $5, $6)
      `,
      [
        deviceId,
        latitude,
        longitude,
        speed || 0,
        ignition || false,
        recordedAt || new Date(),
      ]
    );

    res.json({ success: true });
  } catch (err) {
    console.error("telemetry error:", err);
    res.status(500).json({
      success: false,
      message: "Failed to ingest telemetry",
    });
  }
};