import mariadb from 'mariadb';
import { Pool } from 'pg';

// ----------------------
// 1. MariaDB Connection
// ----------------------
const mariaPool = mariadb.createPool({
  host: process.env.MARIA_HOST,
  user: process.env.MARIA_USER,
  password: process.env.MARIA_PASSWORD,
  database: 'uradi',
  connectionLimit: 10,
});

// ----------------------
// 2. PostgreSQL Connection
// ----------------------
const pgPool = new Pool({
  host: process.env.PG_HOST,
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  database: process.env.PG_DATABASE,
  port: process.env.PG_PORT || 5432,
});

// ----------------------
// 3. Config
// ----------------------
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '50', 10);
const SYNC_INTERVAL = parseInt(process.env.SYNC_INTERVAL || '5000', 10); // ms

// Track last sync
let lastDeviceUpdate = new Date(0);      // Date for device updates
let lastTelemetryTime = new Date(0);     // Date for telemetry

// ----------------------
// 4. Smart sync function
// ----------------------
async function syncMariaToPostgres() {
  console.log(`📝 Sync start | PostgreSQL host: ${process.env.PG_HOST} | ${new Date().toISOString()}`);

  let mariaConn;
  const pgClient = await pgPool.connect();

  try {
    mariaConn = await mariaPool.getConnection();

    // ----------------------------
    // 4a. Fetch new/updated devices
    // ----------------------------
    const devices = await mariaConn.query(
      `SELECT d.id, d.uniqueid, d.name, d.phone, d.model, d.contact, d.category, d.disabled, d.createdat, d.lastupdate,
              r.reg_no, r.vmodel, r.simno, r.owner_name, r.owner_contact
       FROM device d
       LEFT JOIN registration r ON r.serial = d.uniqueid
       WHERE d.lastupdate > ?
       ORDER BY d.lastupdate ASC
       LIMIT ?`,
      [lastDeviceUpdate, BATCH_SIZE]
    );

    if (devices.length > 0) {
      lastDeviceUpdate = new Date(devices[devices.length - 1].lastupdate);

      const deviceValues = [];
      const placeholders = [];

      devices.forEach((d, i) => {
        deviceValues.push(
          d.uniqueid,         // device_uid
          d.reg_no || d.name, // label
          d.simno || d.phone, // sim_number
          d.category || null, // protocol_type
          d.model || null,    // model
          d.contact || null,  // contact
          d.disabled === 1,   // disabled boolean
          d.owner_name || null,
          d.owner_contact || null,
          d.vmodel || null
        );

        placeholders.push(
          `($${i * 10 + 1}, $${i * 10 + 2}, $${i * 10 + 3}, $${i * 10 + 4}, $${i * 10 + 5}, $${i * 10 + 6}, $${i * 10 + 7}, $${i * 10 + 8}, $${i * 10 + 9}, $${i * 10 + 10})`
        );
      });

      const deviceInsertQuery = `
        INSERT INTO devices (device_uid, label, sim_number, protocol_type, model, contact, disabled, owner_name, owner_contact, vmodel)
        VALUES ${placeholders.join(', ')}
        ON CONFLICT (device_uid)
        DO UPDATE SET
          label = EXCLUDED.label,
          sim_number = EXCLUDED.sim_number,
          protocol_type = EXCLUDED.protocol_type,
          model = EXCLUDED.model,
          contact = EXCLUDED.contact,
          disabled = EXCLUDED.disabled,
          owner_name = EXCLUDED.owner_name,
          owner_contact = EXCLUDED.owner_contact,
          vmodel = EXCLUDED.vmodel
      `;

      await pgClient.query(deviceInsertQuery, deviceValues);
      console.log(`✅ Synced ${devices.length} devices`);
    }

    // ----------------------------
    // 4b. Fetch incremental telemetry
    // ----------------------------
    const telemetry = await mariaConn.query(
      `SELECT e.deviceid, e.latitude, e.longitude, e.altitude, e.speed, e.course, e.address, e.attributes,
              e.accuracy, e.network, e.statuscode, e.alarmcode, e.speedlimit, e.odometer,
              e.isRead, e.signalwireconnected, e.powerwireconnected, e.servertime AS device_time
       FROM eventData e
       WHERE e.servertime > ?
       ORDER BY e.servertime ASC
       LIMIT ?`,
      [lastTelemetryTime, BATCH_SIZE]
    );

    if (telemetry.length > 0) {
      lastTelemetryTime = new Date(telemetry[telemetry.length - 1].device_time);

      const teleValues = [];
      const telePlaceholders = [];

      telemetry.forEach((t, i) => {
        teleValues.push(
          t.deviceid,
          t.latitude,
          t.longitude,
          t.altitude,
          t.speed,
          t.course,
          t.address,
          t.attributes,
          t.accuracy,
          t.network,
          t.statuscode === 1,
          t.alarmcode || null,
          t.speedlimit,
          t.odometer,
          t.isRead === 1,
          t.signalwireconnected === 1,
          t.powerwireconnected === 1,
          t.device_time
        );

        telePlaceholders.push(
          `($${i * 18 + 1}, $${i * 18 + 2}, $${i * 18 + 3}, $${i * 18 + 4}, $${i * 18 + 5}, $${i * 18 + 6}, $${i * 18 + 7}, $${i * 18 + 8}, $${i * 18 + 9}, $${i * 18 + 10}, $${i * 18 + 11}, $${i * 18 + 12}, $${i * 18 + 13}, $${i * 18 + 14}, $${i * 18 + 15}, $${i * 18 + 16}, $${i * 18 + 17}, $${i * 18 + 18})`
        );
      });

      const teleInsertQuery = `
        INSERT INTO telemetry (
          device_id, latitude, longitude, altitude, speed_kph, course, address, attributes,
          accuracy, network, statuscode, alarmcode, speedlimit, odometer, isread,
          signalwireconnected, powerwireconnected, device_time
        )
        VALUES ${telePlaceholders.join(', ')}
        ON CONFLICT (device_id, device_time) DO NOTHING
      `;

      await pgClient.query(teleInsertQuery, teleValues);
      console.log(`✅ Synced ${telemetry.length} telemetry records`);
    }

  } catch (err) {
    console.error('❌ Sync error:', err.message || err);
  } finally {
    if (mariaConn) mariaConn.end();
    pgClient.release();
  }
}

// ----------------------------
// 5. Continuous Loop
// ----------------------------
async function runLoop() {
  while (true) {
    await syncMariaToPostgres();
    await new Promise(res => setTimeout(res, SYNC_INTERVAL));
  }
}

// ----------------------------
// 6. Run standalone
// ----------------------------
if (require.main === module) {
  runLoop().catch(err => console.error('❌ Fatal sync loop error:', err));
}