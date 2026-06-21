const mqtt = require('mqtt');

const MQTT_BROKER = process.env.MQTT_BROKER || 'mqtt://localhost:1883';
const TOPIC = 'greenhouse/sensors';
const INTERVAL = 2000;

const sensors = {
  temperature: { min: 15, max: 35, current: 25, unit: '°C', name: '温度' },
  humidity: { min: 30, max: 80, current: 55, unit: '%', name: '湿度' },
  light: { min: 1000, max: 80000, current: 30000, unit: 'lux', name: '光照' },
  soil: { min: 20, max: 90, current: 60, unit: '%', name: '土壤湿度' },
  co2: { min: 400, max: 2000, current: 800, unit: 'ppm', name: 'CO₂' }
};

function randomWalk(sensor) {
  const range = sensor.max - sensor.min;
  const step = range * 0.02;
  let next = sensor.current + (Math.random() - 0.5) * 2 * step;
  if (next < sensor.min) next = sensor.min;
  if (next > sensor.max) next = sensor.max;
  sensor.current = next;
  return Math.round(next * 100) / 100;
}

const client = mqtt.connect(MQTT_BROKER, {
  clientId: 'greenhouse-simulator-' + Math.random().toString(16).slice(2, 10),
  will: {
    topic: 'greenhouse/status',
    payload: JSON.stringify({ status: 'offline', reason: 'unexpected_disconnect' }),
    qos: 1,
    retain: true
  }
});

client.on('connect', () => {
  console.log(`[模拟器] 已连接到 MQTT Broker: ${MQTT_BROKER}`);
  console.log(`[模拟器] 推送主题: ${TOPIC}, 间隔: ${INTERVAL}ms`);

  client.publish('greenhouse/status', JSON.stringify({
    status: 'online',
    sensors: Object.keys(sensors),
    timestamp: Date.now()
  }), { retain: true, qos: 1 });

  setInterval(() => {
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

    client.publish(TOPIC, JSON.stringify(payload), { qos: 0 });
    console.log(`[${new Date().toISOString()}] 推送: T=${payload.readings.temperature.value}°C H=${payload.readings.humidity.value}% L=${payload.readings.light.value}lux S=${payload.readings.soil.value}% C=${payload.readings.co2.value}ppm`);
  }, INTERVAL);
});

client.on('error', (err) => {
  console.error('[模拟器] 连接错误:', err.message);
});

client.on('close', () => {
  console.log('[模拟器] 连接已关闭');
});

process.on('SIGINT', () => {
  console.log('\n[模拟器] 正在优雅退出...');
  client.publish('greenhouse/status', JSON.stringify({
    status: 'offline',
    reason: 'shutdown',
    timestamp: Date.now()
  }), { retain: true, qos: 1 }, () => {
    client.end();
    process.exit(0);
  });
});
