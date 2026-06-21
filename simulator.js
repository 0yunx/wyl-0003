const mqtt = require('mqtt');
const fs = require('fs');
const path = require('path');

const MQTT_BROKER = process.env.MQTT_BROKER || 'mqtt://localhost:1883';
const TOPIC = 'greenhouse/sensors';
const INTERVAL = 2000;

const SIGNAL_INTERVAL_MIN = 25000;
const SIGNAL_INTERVAL_MAX = 45000;
const FREEZE_MIN_MS = 5000;
const FREEZE_MAX_MS = 8000;
const SIGNAL_RATIO = 0.5;

const QUEUE_FILE = path.join(__dirname, '.simulator_queue.json');

const sensors = {
  temperature: { min: 15, max: 35, current: 25, unit: '°C', name: '温度' },
  humidity: { min: 30, max: 80, current: 55, unit: '%', name: '湿度' },
  light: { min: 1000, max: 80000, current: 30000, unit: 'lux', name: '光照' },
  soil: { min: 20, max: 90, current: 60, unit: '%', name: '土壤湿度' },
  co2: { min: 400, max: 2000, current: 800, unit: 'ppm', name: 'CO₂' }
};

let localQueue = [];
let publishTimer = null;
let signalTimer = null;
let clientConnected = false;
let flushing = false;

function loadQueue() {
  try {
    if (fs.existsSync(QUEUE_FILE)) {
      const raw = fs.readFileSync(QUEUE_FILE, 'utf8');
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) {
        localQueue = parsed;
        console.log(`[模拟器] 从磁盘恢复 ${localQueue.length} 条待补发数据`);
      }
    }
  } catch (e) {
    console.warn('[模拟器] 加载本地队列失败:', e.message);
  }
}

function saveQueue() {
  try {
    fs.writeFileSync(QUEUE_FILE, JSON.stringify(localQueue), 'utf8');
  } catch (e) {
    console.warn('[模拟器] 保存本地队列失败:', e.message);
  }
}

function enqueueToLocal(payload) {
  localQueue.push(payload);
  if (localQueue.length % 50 === 0) saveQueue();
}

function randomWalk(sensor) {
  const range = sensor.max - sensor.min;
  const step = range * 0.02;
  let next = sensor.current + (Math.random() - 0.5) * 2 * step;
  if (next < sensor.min) next = sensor.min;
  if (next > sensor.max) next = sensor.max;
  sensor.current = next;
  return Math.round(next * 100) / 100;
}

function generatePayload() {
  const payload = {
    deviceId: 'greenhouse-edge-001',
    timestamp: Date.now(),
    readings: {}
  };
  for (const [key, sensor] of Object.entries(sensors)) {
    payload.readings[key] = {
      value: randomWalk(sensor),
      unit: sensor.unit,
      name: sensor.name
    };
  }
  return payload;
}

function publishOrEnqueue(payload) {
  const json = JSON.stringify(payload);
  if (clientConnected && client.connected) {
    try {
      client.publish(TOPIC, json, { qos: 0 }, (err) => {
        if (err) {
          enqueueToLocal(payload);
        }
      });
    } catch (e) {
      enqueueToLocal(payload);
    }
  } else {
    enqueueToLocal(payload);
  }
}

function flushLocalQueue() {
  if (flushing) return;
  if (localQueue.length === 0) return;
  if (!clientConnected || !client.connected) return;

  flushing = true;
  const total = localQueue.length;
  console.log(`[模拟器] 开始补发 ${total} 条积压数据...`);

  let index = 0;
  const flushBatch = () => {
    const BATCH = 50;
    let sent = 0;
    while (index < localQueue.length && sent < BATCH) {
      const p = localQueue[index];
      try {
        client.publish(TOPIC, JSON.stringify(p), { qos: 0 });
      } catch (e) {
        break;
      }
      index++;
      sent++;
    }

    if (index >= localQueue.length) {
      localQueue = [];
      saveQueue();
      flushing = false;
      console.log(`[模拟器] 补发完成，共 ${total} 条`);
    } else {
      setImmediate(flushBatch);
    }
  };
  flushBatch();
}

function injectSignal() {
  if (!clientConnected) return;

  const nextSignal = SIGNAL_INTERVAL_MIN + Math.random() * (SIGNAL_INTERVAL_MAX - SIGNAL_INTERVAL_MIN);

  signalTimer = setTimeout(() => {
    const mode = Math.random() < SIGNAL_RATIO ? 'hangup' : 'freeze';

    if (mode === 'hangup') {
      console.log('\x1b[33m[SIGNAL] 注入 socket hangup —— 模拟网络中断\x1b[0m');
      try {
        if (client.stream && client.stream.destroy) {
          client.stream.destroy(new Error('ECONNRESET: simulated hangup'));
        } else {
          client.end(true);
        }
      } catch (e) {
        try { client.end(true); } catch (_) {}
      }
    } else {
      const ms = FREEZE_MIN_MS + Math.random() * (FREEZE_MAX_MS - FREEZE_MIN_MS);
      console.log(`\x1b[33m[SIGNAL] 注入进程卡死 —— sleep ${Math.round(ms)}ms\x1b[0m`);
      const endAt = Date.now() + ms;
      while (Date.now() < endAt) {}
      console.log('\x1b[33m[SIGNAL] 进程恢复\x1b[0m');
    }

    injectSignal();
  }, nextSignal);

  console.log(`[模拟器] 下一次 signal 将在 ${Math.round(nextSignal / 1000)}s 后`);
}

const client = mqtt.connect(MQTT_BROKER, {
  clientId: 'greenhouse-simulator-' + Math.random().toString(16).slice(2, 10),
  will: {
    topic: 'greenhouse/status',
    payload: JSON.stringify({ status: 'offline', reason: 'unexpected_disconnect' }),
    qos: 1,
    retain: true
  },
  reconnectPeriod: 1500,
  connectTimeout: 5000
});

client.on('connect', () => {
  clientConnected = true;
  console.log(`[模拟器] 已连接到 MQTT Broker: ${MQTT_BROKER}`);
  console.log(`[模拟器] 推送主题: ${TOPIC}, 间隔: ${INTERVAL}ms`);

  client.publish('greenhouse/status', JSON.stringify({
    status: 'online',
    sensors: Object.keys(sensors),
    timestamp: Date.now()
  }), { retain: true, qos: 1 });

  flushLocalQueue();

  if (!publishTimer) {
    publishTimer = setInterval(() => {
      const payload = generatePayload();
      publishOrEnqueue(payload);
      console.log(`[${new Date(payload.timestamp).toISOString()}] 推送: T=${payload.readings.temperature.value}°C H=${payload.readings.humidity.value}% L=${payload.readings.light.value}lux S=${payload.readings.soil.value}% C=${payload.readings.co2.value}ppm` + (localQueue.length > 0 ? ` [待补发:${localQueue.length}]` : ''));
    }, INTERVAL);
  }

  if (!signalTimer) {
    setTimeout(injectSignal, 8000);
  }
});

client.on('reconnect', () => {
  console.log('[模拟器] 正在尝试重连...');
});

client.on('close', () => {
  const wasConnected = clientConnected;
  clientConnected = false;
  if (wasConnected) {
    console.log('[模拟器] 连接已关闭，进入离线模式（数据入本地队列）');
  }
});

client.on('error', (err) => {
  console.error('[模拟器] 连接错误:', err.message);
});

client.on('offline', () => {
  clientConnected = false;
  console.log('[模拟器] 进入离线状态');
});

process.on('SIGINT', () => {
  console.log('\n[模拟器] 正在优雅退出...');
  saveQueue();
  if (signalTimer) { clearTimeout(signalTimer); signalTimer = null; }
  if (publishTimer) { clearInterval(publishTimer); publishTimer = null; }
  client.publish('greenhouse/status', JSON.stringify({
    status: 'offline',
    reason: 'shutdown',
    timestamp: Date.now()
  }), { retain: true, qos: 1 }, () => {
    client.end(false, {}, () => {
      process.exit(0);
    });
  });
});

process.on('exit', () => {
  saveQueue();
});

loadQueue();
