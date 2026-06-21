const express = require('express');
const http = require('http');
const path = require('path');
const Aedes = require('aedes');
const net = require('net');
const mqtt = require('mqtt');
const { DatabaseSync } = require('node:sqlite');

const PORT = process.env.PORT || 3000;
const MQTT_PORT = process.env.MQTT_PORT || 1883;
const DB_PATH = path.join(__dirname, 'db.sqlite');
const OFFLINE_TIMEOUT = 10000;

const app = express();
const server = http.createServer(app);

const aedes = new Aedes();
const mqttServer = net.createServer(aedes.handle);

const db = new DatabaseSync(DB_PATH);

db.exec(`
  CREATE TABLE IF NOT EXISTS sensor_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id TEXT NOT NULL,
    sensor_type TEXT NOT NULL,
    value REAL NOT NULL,
    unit TEXT,
    timestamp INTEGER NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_sensor_timestamp ON sensor_data(sensor_type, timestamp);
  CREATE INDEX IF NOT EXISTS idx_timestamp ON sensor_data(timestamp);
`);

const configStmt = db.exec(`
  CREATE TABLE IF NOT EXISTS config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
  )
`);

const defaultThresholds = {
  temperature: { min: 10, max: 35 },
  humidity: { min: 30, max: 70 },
  light: { min: 5000, max: 60000 },
  soil: { min: 30, max: 80 },
  co2: { min: 400, max: 1500 }
};

let thresholds = loadThresholds();

function loadThresholds() {
  try {
    const stmt = db.prepare('SELECT value FROM config WHERE key = ?');
    const row = stmt.get('thresholds');
    if (row) {
      return JSON.parse(row.value);
    }
  } catch (e) {}
  saveThresholds(defaultThresholds);
  return { ...defaultThresholds };
}

function saveThresholds(th) {
  const stmt = db.prepare('INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)');
  stmt.run('thresholds', JSON.stringify(th));
}

const insertStmt = db.prepare(
  'INSERT INTO sensor_data (device_id, sensor_type, value, unit, timestamp) VALUES (?, ?, ?, ?, ?)'
);

const latestData = {};
const lastSeen = {};

function isDeviceOnline(deviceId) {
  const last = lastSeen[deviceId];
  if (!last) return false;
  return Date.now() - last < OFFLINE_TIMEOUT;
}

function checkThreshold(sensorType, value) {
  const th = thresholds[sensorType];
  if (!th) return { alert: false };
  const alert = value < th.min || value > th.max;
  const direction = value < th.min ? 'low' : (value > th.max ? 'high' : null);
  return { alert, direction, threshold: th };
}

const mqttClient = mqtt.connect(`mqtt://localhost:${MQTT_PORT}`, {
  clientId: 'server-subscriber-' + Math.random().toString(16).slice(2, 10)
});

mqttClient.on('connect', () => {
  console.log(`[服务端] MQTT 客户端已连接，订阅主题...`);
  mqttClient.subscribe('greenhouse/sensors', { qos: 0 });
  mqttClient.subscribe('greenhouse/status', { qos: 1 });
});

mqttClient.on('message', (topic, message) => {
  try {
    const data = JSON.parse(message.toString());

    if (topic === 'greenhouse/status') {
      console.log(`[状态] 设备状态: ${data.status}`);
      if (data.status === 'online') {
        lastSeen[data.deviceId || 'greenhouse-edge-001'] = Date.now();
      } else if (data.status === 'offline') {
        lastSeen[data.deviceId || 'greenhouse-edge-001'] = 0;
      }
      return;
    }

    if (topic === 'greenhouse/sensors') {
      const deviceId = data.deviceId || 'unknown';
      const timestamp = data.timestamp || Date.now();
      lastSeen[deviceId] = timestamp;

      for (const [sensorType, reading] of Object.entries(data.readings)) {
        insertStmt.run(deviceId, sensorType, reading.value, reading.unit, timestamp);

        const alertInfo = checkThreshold(sensorType, reading.value);
        latestData[sensorType] = {
          value: reading.value,
          unit: reading.unit,
          name: reading.name,
          timestamp,
          alert: alertInfo.alert,
          alertDirection: alertInfo.direction
        };
      }
    }
  } catch (err) {
    console.error('[MQTT] 消息解析错误:', err.message);
  }
});

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

app.get('/api/latest', (req, res) => {
  const deviceId = 'greenhouse-edge-001';
  const online = isDeviceOnline(deviceId);
  res.json({
    deviceId,
    online,
    lastSeen: lastSeen[deviceId] || null,
    sensors: latestData,
    thresholds
  });
});

app.get('/api/history', (req, res) => {
  const { sensor, from, to, limit } = req.query;

  let sql = 'SELECT sensor_type, value, unit, timestamp FROM sensor_data WHERE 1=1';
  const params = [];

  if (sensor) {
    sql += ' AND sensor_type = ?';
    params.push(sensor);
  }
  if (from) {
    sql += ' AND timestamp >= ?';
    params.push(parseInt(from));
  }
  if (to) {
    sql += ' AND timestamp <= ?';
    params.push(parseInt(to));
  }

  sql += ' ORDER BY timestamp ASC';

  if (limit) {
    sql += ' LIMIT ?';
    params.push(parseInt(limit));
  }

  try {
    const stmt = db.prepare(sql);
    const rows = stmt.all(...params);
    res.json({ count: rows.length, data: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/config', (req, res) => {
  res.json({ thresholds });
});

app.post('/api/config', (req, res) => {
  const { thresholds: newThresholds } = req.body;
  if (!newThresholds || typeof newThresholds !== 'object') {
    return res.status(400).json({ error: '无效的配置格式' });
  }

  for (const sensor of Object.keys(thresholds)) {
    if (newThresholds[sensor]) {
      if (typeof newThresholds[sensor].min === 'number') {
        thresholds[sensor].min = newThresholds[sensor].min;
      }
      if (typeof newThresholds[sensor].max === 'number') {
        thresholds[sensor].max = newThresholds[sensor].max;
      }
    }
  }

  saveThresholds(thresholds);
  res.json({ success: true, thresholds });
});

app.get('/api/status', (req, res) => {
  const deviceId = 'greenhouse-edge-001';
  res.json({
    deviceId,
    online: isDeviceOnline(deviceId),
    lastSeen: lastSeen[deviceId] || null,
    offlineTimeout: OFFLINE_TIMEOUT
  });
});

mqttServer.listen(MQTT_PORT, () => {
  console.log(`[MQTT] Broker 已启动，端口: ${MQTT_PORT}`);
});

server.listen(PORT, () => {
  console.log(`[HTTP] 服务器已启动，端口: ${PORT}`);
  console.log(`[HTTP] 前端页面: http://localhost:${PORT}`);
  console.log(`[HTTP] API: /api/latest, /api/history, /api/config, /api/status`);
});

process.on('SIGINT', () => {
  console.log('\n[服务端] 正在优雅关闭...');
  mqttClient.end();
  aedes.close(() => {
    mqttServer.close();
    server.close(() => {
      db.close();
      process.exit(0);
    });
  });
});
