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
const DEVICE_CHECK_INTERVAL = 2000;

const app = express();
const server = http.createServer(app);

const aedes = new Aedes();
const mqttServer = net.createServer(aedes.handle);

const db = new DatabaseSync(DB_PATH);

const migrations = [
  {
    version: 1,
    description: '初始版本：sensor_data 基础表 + config 表',
    up: (db) => {
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

        CREATE TABLE IF NOT EXISTS config (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        );
      `);
    }
  },
  {
    version: 2,
    description: '新增告警相关字段 + alerts 告警事件表',
    up: (db) => {
      db.exec(`
        ALTER TABLE sensor_data ADD COLUMN alert INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE sensor_data ADD COLUMN alert_direction TEXT;
        ALTER TABLE sensor_data ADD COLUMN threshold_min REAL;
        ALTER TABLE sensor_data ADD COLUMN threshold_max REAL;
      `);
      db.exec(`
        CREATE TABLE IF NOT EXISTS alerts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          device_id TEXT NOT NULL,
          sensor_type TEXT NOT NULL,
          direction TEXT NOT NULL,
          value REAL NOT NULL,
          threshold_min REAL,
          threshold_max REAL,
          started_at INTEGER NOT NULL,
          resolved_at INTEGER,
          duration INTEGER,
          status TEXT NOT NULL DEFAULT 'active'
        );
        CREATE INDEX IF NOT EXISTS idx_alerts_sensor ON alerts(sensor_type, started_at);
        CREATE INDEX IF NOT EXISTS idx_alerts_status ON alerts(status);
      `);
    }
  },
  {
    version: 3,
    description: '新增 alerts 时间索引 + sensor_data 告警查询索引',
    up: (db) => {
      db.exec(`
        CREATE INDEX IF NOT EXISTS idx_alerts_started_at ON alerts(started_at);
        CREATE INDEX IF NOT EXISTS idx_sensor_alert ON sensor_data(sensor_type, alert, timestamp);
      `);
    }
  }
];

const LATEST_SCHEMA_VERSION = migrations[migrations.length - 1].version;
const HISTORY_DEFAULT_LIMIT = 1000;
const HISTORY_MAX_LIMIT = 10000;
const ALERTS_DEFAULT_LIMIT = 100;
const ALERTS_MAX_LIMIT = 500;

function getSchemaVersion() {
  try {
    const checkTable = db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?");
    const hasSchemaVersion = !!checkTable.get('schema_version');

    if (hasSchemaVersion) {
      const stmt = db.prepare('SELECT MAX(version) as v FROM schema_version');
      const r = stmt.get();
      if (r && r.v !== null && r.v !== undefined) {
        return r.v;
      }
    }

    const hasSensorData = !!checkTable.get('sensor_data');
    if (!hasSensorData) return 0;

    const hasAlerts = !!checkTable.get('alerts');
    if (!hasAlerts) return 1;

    try {
      const checkCol = db.prepare('SELECT alert FROM sensor_data LIMIT 1');
      checkCol.get();
      return 2;
    } catch (e) {
      return 1;
    }
  } catch (e) {
    return 0;
  }
}

function runMigrations() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS schema_version (
      version INTEGER PRIMARY KEY,
      applied_at INTEGER NOT NULL
    )
  `);

  const currentVersion = getSchemaVersion();
  console.log(`[DB] 当前 schema 版本: v${currentVersion}, 最新版本: v${LATEST_SCHEMA_VERSION}`);

  if (currentVersion >= LATEST_SCHEMA_VERSION) {
    console.log('[DB] Schema 已是最新，无需迁移');
    return;
  }

  for (const migration of migrations) {
    if (migration.version > currentVersion) {
      console.log(`[DB] 正在迁移到 v${migration.version}: ${migration.description}`);
      try {
        migration.up(db);
        const stmt = db.prepare('INSERT OR REPLACE INTO schema_version (version, applied_at) VALUES (?, ?)');
        stmt.run(migration.version, Date.now());
        console.log(`[DB] 迁移到 v${migration.version} 完成 ✓`);
      } catch (err) {
        console.error(`[DB] 迁移到 v${migration.version} 失败:`, err.message);
        throw err;
      }
    }
  }
  console.log('[DB] 全部迁移完成');
}

runMigrations();

const defaultThresholds = {
  temperature: { min: 10, max: 35 },
  humidity: { min: 30, max: 70 },
  light: { min: 5000, max: 60000 },
  soil: { min: 30, max: 80 },
  co2: { min: 400, max: 1500 }
};

let thresholds = loadThresholds();
const activeAlerts = {};
const latestData = {};
const lastSeen = {};
let deviceOnline = false;

function loadThresholds() {
  try {
    const stmt = db.prepare('SELECT value FROM config WHERE key = ?');
    const row = stmt.get('thresholds');
    if (row) {
      return JSON.parse(row.value);
    }
  } catch (e) {}
  saveThresholds(defaultThresholds);
  return JSON.parse(JSON.stringify(defaultThresholds));
}

function saveThresholds(th) {
  const stmt = db.prepare('INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)');
  stmt.run('thresholds', JSON.stringify(th));
}

let insertSensorStmt;
let insertAlertStmt;
let resolveAlertStmt;

function prepareStatements() {
  insertSensorStmt = db.prepare(
    'INSERT INTO sensor_data (device_id, sensor_type, value, unit, alert, alert_direction, threshold_min, threshold_max, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'
  );
  insertAlertStmt = db.prepare(
    'INSERT INTO alerts (device_id, sensor_type, direction, value, threshold_min, threshold_max, started_at, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
  );
  resolveAlertStmt = db.prepare(
    'UPDATE alerts SET status = ?, resolved_at = ?, duration = ? WHERE id = ?'
  );
}

prepareStatements();

function isDeviceOnline(deviceId) {
  const last = lastSeen[deviceId];
  if (!last) return false;
  return Date.now() - last < OFFLINE_TIMEOUT;
}

function checkThreshold(sensorType, value) {
  const th = thresholds[sensorType];
  if (!th) return { alert: false, direction: null, threshold: null };
  const alert = value < th.min || value > th.max;
  const direction = value < th.min ? 'low' : (value > th.max ? 'high' : null);
  return { alert, direction, threshold: th };
}

function processAlertTransition(deviceId, sensorType, value, alertInfo, timestamp) {
  const wasAlert = !!activeAlerts[sensorType];
  const isAlert = alertInfo.alert;

  if (isAlert && !wasAlert) {
    const info = insertAlertStmt.run(
      deviceId, sensorType, alertInfo.direction, value,
      alertInfo.threshold.min, alertInfo.threshold.max, timestamp, 'active'
    );
    activeAlerts[sensorType] = {
      id: info.lastInsertRowid,
      direction: alertInfo.direction,
      startedAt: timestamp
    };
    console.log(`[告警] ${sensorType} 触发${alertInfo.direction === 'high' ? '高' : '低'}值告警，值=${value}`);
  } else if (!isAlert && wasAlert) {
    const alert = activeAlerts[sensorType];
    const duration = timestamp - alert.startedAt;
    resolveAlertStmt.run('resolved', timestamp, duration, alert.id);
    delete activeAlerts[sensorType];
    console.log(`[告警] ${sensorType} 恢复正常，持续 ${Math.round(duration / 1000)} 秒`);
  }
}

function resolveAllActiveAlerts(reason) {
  const sensorTypes = Object.keys(activeAlerts);
  if (sensorTypes.length === 0) return 0;

  const now = Date.now();
  for (const sensorType of sensorTypes) {
    const alert = activeAlerts[sensorType];
    const duration = now - alert.startedAt;
    resolveAlertStmt.run(reason, now, duration, alert.id);
    delete activeAlerts[sensorType];
  }
  console.log(`[告警] 已结算 ${sensorTypes.length} 条活跃告警，原因: ${reason}`);
  return sensorTypes.length;
}

function checkDeviceOffline() {
  const deviceId = 'greenhouse-edge-001';
  const wasOnline = deviceOnline;
  const isOnline = isDeviceOnline(deviceId);
  deviceOnline = isOnline;

  if (wasOnline && !isOnline) {
    console.log('[状态] 设备超时判定离线');
    resolveAllActiveAlerts('timeout_offline');
  }
}

const mqttClient = mqtt.connect(`mqtt://localhost:${MQTT_PORT}`, {
  clientId: 'server-subscriber-' + Math.random().toString(16).slice(2, 10)
});

mqttClient.on('connect', () => {
  console.log(`[服务端] MQTT 客户端已连接，订阅主题...`);
  mqttClient.subscribe('greenhouse/sensors', { qos: 0 });
  mqttClient.subscribe('greenhouse/status', { qos: 1 });

  try {
    const activeStmt = db.prepare('SELECT id, sensor_type, direction, started_at FROM alerts WHERE status = ?');
    const rows = activeStmt.all('active');
    for (const row of rows) {
      activeAlerts[row.sensor_type] = {
        id: row.id,
        direction: row.direction,
        startedAt: row.started_at
      };
    }
    console.log(`[告警] 从数据库恢复 ${rows.length} 条活跃告警`);
  } catch (e) {
    console.warn('[告警] 恢复活跃告警失败:', e.message);
  }
});

mqttClient.on('message', (topic, message) => {
  try {
    const data = JSON.parse(message.toString());

    if (topic === 'greenhouse/status') {
      console.log(`[状态] 设备状态: ${data.status}`);
      const deviceId = data.deviceId || 'greenhouse-edge-001';
      if (data.status === 'online') {
        lastSeen[deviceId] = Date.now();
        deviceOnline = true;
      } else if (data.status === 'offline') {
        lastSeen[deviceId] = 0;
        deviceOnline = false;
        resolveAllActiveAlerts('device_offline');
      }
      return;
    }

    if (topic === 'greenhouse/sensors') {
      const deviceId = data.deviceId || 'unknown';
      const timestamp = data.timestamp || Date.now();
      lastSeen[deviceId] = timestamp;
      deviceOnline = true;

      for (const [sensorType, reading] of Object.entries(data.readings)) {
        const alertInfo = checkThreshold(sensorType, reading.value);
        const th = alertInfo.threshold;

        insertSensorStmt.run(
          deviceId, sensorType, reading.value, reading.unit,
          alertInfo.alert ? 1 : 0, alertInfo.direction,
          th ? th.min : null, th ? th.max : null, timestamp
        );

        processAlertTransition(deviceId, sensorType, reading.value, alertInfo, timestamp);

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
    console.error('[MQTT] 消息处理错误:', err.message);
  }
});

setInterval(checkDeviceOffline, DEVICE_CHECK_INTERVAL);

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

function sendError(res, statusCode, message, code) {
  res.status(statusCode).json({
    success: false,
    error: {
      code: code || 'INTERNAL_ERROR',
      message: message || 'Internal Server Error'
    }
  });
}

function sendSuccess(res, data, statusCode) {
  res.status(statusCode || 200).json({
    success: true,
    data
  });
}

app.get('/api/latest', (req, res) => {
  try {
    const deviceId = 'greenhouse-edge-001';
    const online = isDeviceOnline(deviceId);
    res.json({
      success: true,
      data: {
        deviceId,
        online,
        lastSeen: lastSeen[deviceId] || null,
        sensors: latestData,
        activeAlerts: Object.keys(activeAlerts).length,
        thresholds
      }
    });
  } catch (err) {
    console.error('[API /api/latest]', err.message);
    sendError(res, 500, '获取最新数据失败', 'FETCH_FAILED');
  }
});

app.get('/api/history', (req, res) => {
  try {
    const { sensor, from, to, limit } = req.query;

    let parsedLimit = limit ? parseInt(limit) : HISTORY_DEFAULT_LIMIT;
    if (isNaN(parsedLimit) || parsedLimit < 1) parsedLimit = HISTORY_DEFAULT_LIMIT;
    if (parsedLimit > HISTORY_MAX_LIMIT) parsedLimit = HISTORY_MAX_LIMIT;

    let sql = 'SELECT sensor_type, value, unit, alert, alert_direction, threshold_min, threshold_max, timestamp FROM sensor_data WHERE 1=1';
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

    sql += ' ORDER BY timestamp ASC LIMIT ?';
    params.push(parsedLimit);

    const stmt = db.prepare(sql);
    const rows = stmt.all(...params).map(r => ({
      ...r,
      alert: r.alert === 1
    }));

    res.json({
      success: true,
      data: {
        count: rows.length,
        limit: parsedLimit,
        maxLimit: HISTORY_MAX_LIMIT,
        items: rows
      }
    });
  } catch (err) {
    console.error('[API /api/history]', err.message);
    sendError(res, 500, '查询历史数据失败', 'QUERY_FAILED');
  }
});

app.get('/api/alerts', (req, res) => {
  try {
    const { sensor, status, from, to, limit } = req.query;

    let parsedLimit = limit ? parseInt(limit) : ALERTS_DEFAULT_LIMIT;
    if (isNaN(parsedLimit) || parsedLimit < 1) parsedLimit = ALERTS_DEFAULT_LIMIT;
    if (parsedLimit > ALERTS_MAX_LIMIT) parsedLimit = ALERTS_MAX_LIMIT;

    let sql = 'SELECT * FROM alerts WHERE 1=1';
    const params = [];

    if (sensor) {
      sql += ' AND sensor_type = ?';
      params.push(sensor);
    }
    if (status) {
      sql += ' AND status = ?';
      params.push(status);
    }
    if (from) {
      sql += ' AND started_at >= ?';
      params.push(parseInt(from));
    }
    if (to) {
      sql += ' AND started_at <= ?';
      params.push(parseInt(to));
    }

    sql += ' ORDER BY started_at DESC LIMIT ?';
    params.push(parsedLimit);

    const stmt = db.prepare(sql);
    const rows = stmt.all(...params);

    res.json({
      success: true,
      data: {
        count: rows.length,
        limit: parsedLimit,
        maxLimit: ALERTS_MAX_LIMIT,
        items: rows
      }
    });
  } catch (err) {
    console.error('[API /api/alerts]', err.message);
    sendError(res, 500, '查询告警事件失败', 'QUERY_FAILED');
  }
});

app.get('/api/config', (req, res) => {
  try {
    res.json({
      success: true,
      data: { thresholds }
    });
  } catch (err) {
    console.error('[API /api/config GET]', err.message);
    sendError(res, 500, '获取配置失败', 'FETCH_FAILED');
  }
});

app.post('/api/config', (req, res) => {
  try {
    const { thresholds: newThresholds } = req.body;
    if (!newThresholds || typeof newThresholds !== 'object') {
      return sendError(res, 400, '无效的配置格式', 'INVALID_FORMAT');
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
    res.json({
      success: true,
      data: { thresholds }
    });
  } catch (err) {
    console.error('[API /api/config POST]', err.message);
    sendError(res, 500, '保存配置失败', 'SAVE_FAILED');
  }
});

app.get('/api/status', (req, res) => {
  try {
    const deviceId = 'greenhouse-edge-001';
    res.json({
      success: true,
      data: {
        deviceId,
        online: isDeviceOnline(deviceId),
        lastSeen: lastSeen[deviceId] || null,
        offlineTimeout: OFFLINE_TIMEOUT,
        schemaVersion: LATEST_SCHEMA_VERSION
      }
    });
  } catch (err) {
    console.error('[API /api/status]', err.message);
    sendError(res, 500, '获取状态失败', 'FETCH_FAILED');
  }
});

app.use((req, res) => {
  sendError(res, 404, '接口不存在', 'NOT_FOUND');
});

app.use((err, req, res, next) => {
  console.error('[API 未捕获错误]', err.message);
  sendError(res, 500, '服务器内部错误', 'INTERNAL_ERROR');
});

mqttServer.listen(MQTT_PORT, () => {
  console.log(`[MQTT] Broker 已启动，端口: ${MQTT_PORT}`);
});

server.listen(PORT, () => {
  console.log(`[HTTP] 服务器已启动，端口: ${PORT}`);
  console.log(`[HTTP] 前端页面: http://localhost:${PORT}`);
  console.log(`[HTTP] API: /api/latest, /api/history, /api/alerts, /api/config, /api/status`);
  console.log(`[DB] Schema 版本: v${LATEST_SCHEMA_VERSION}`);
});

process.on('SIGINT', () => {
  console.log('\n[服务端] 正在优雅关闭...');
  resolveAllActiveAlerts('shutdown');
  mqttClient.end();
  aedes.close(() => {
    mqttServer.close();
    server.close(() => {
      db.close();
      process.exit(0);
    });
  });
});
