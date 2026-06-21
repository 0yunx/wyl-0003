const crypto = require('crypto');
const { Router } = require('express');

const RETRY_BATCH = 100;
const RETRY_INTERVAL_MS = 3000;
const MAX_RETRY = 10;
const FLUSH_CONCURRENCY = 20;

function makeFingerprint(deviceId, timestamp) {
  return crypto
    .createHash('sha1')
    .update(`${deviceId}|${timestamp}`)
    .digest('hex');
}

module.exports = function createOfflineQueue({ db, helpers, aedes, schemaVersion }) {
  const state = {
    brokerConnected: false,
    lastSuccessTs: 0,
    lastFailedTs: 0,
    retryTimer: null,
    processing: false,
    pendingCount: 0,
    insertsStmt: null,
    enqueueStmt: null,
    markDoneStmt: null,
    markAttemptStmt: null,
    fetchPendingStmt: null,
    countPendingStmt: null,
    fetchOneStmt: null,
    insertSensorStmt: null,
    insertAlertStmt: null,
    resolveAlertStmt: null,
    activeAlerts: {},
    thresholds: null,
    latestData: null,
    lastSeen: null,
    processAlertTransition: null,
    checkThreshold: null
  };

  function prepareStatements() {
    state.enqueueStmt = db.prepare(`
      INSERT OR IGNORE INTO offline_queue
      (device_id, payload, timestamp, fingerprint, retry_count, status, created_at)
      VALUES (?, ?, ?, ?, 0, 'pending', ?)
    `);

    state.markDoneStmt = db.prepare(`
      UPDATE offline_queue SET status = 'done', last_attempt_at = ? WHERE fingerprint = ?
    `);

    state.markAttemptStmt = db.prepare(`
      UPDATE offline_queue SET retry_count = retry_count + 1, last_attempt_at = ?,
        status = CASE WHEN retry_count + 1 >= ? THEN 'failed' ELSE status END
      WHERE fingerprint = ? AND status != 'done'
    `);

    state.fetchPendingStmt = db.prepare(`
      SELECT fingerprint, device_id, payload, timestamp, retry_count
      FROM offline_queue
      WHERE status = 'pending'
      ORDER BY created_at ASC LIMIT ?
    `);

    state.countPendingStmt = db.prepare(`
      SELECT COUNT(*) as c FROM offline_queue WHERE status = 'pending'
    `);

    state.fetchOneStmt = db.prepare(`
      SELECT fingerprint, status FROM offline_queue WHERE fingerprint = ? LIMIT 1
    `);

    state.insertSensorStmt = db.prepare(
      'INSERT OR IGNORE INTO sensor_data (device_id, sensor_type, value, unit, alert, alert_direction, threshold_min, threshold_max, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'
    );

    state.insertAlertStmt = db.prepare(
      'INSERT INTO alerts (device_id, sensor_type, direction, value, threshold_min, threshold_max, started_at, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
    );

    state.resolveAlertStmt = db.prepare(
      'UPDATE alerts SET status = ?, resolved_at = ?, duration = ? WHERE id = ?'
    );
  }

  function setSharedRefs(refs) {
    state.activeAlerts = refs.activeAlerts;
    state.thresholds = refs.thresholds;
    state.latestData = refs.latestData;
    state.lastSeen = refs.lastSeen;
    state.processAlertTransition = refs.processAlertTransition;
    state.checkThreshold = refs.checkThreshold;
  }

  function refreshPendingCount() {
    try {
      state.pendingCount = (state.countPendingStmt.get() || {}).c || 0;
    } catch (e) {
      console.warn('[offline] count pending failed:', e.message);
    }
  }

  function isAlreadyDone(fp) {
    try {
      const row = state.fetchOneStmt.get(fp);
      return row && row.status === 'done';
    } catch (e) {
      return false;
    }
  }

  function processPayloadInternal(data, { realtime = true } = {}) {
    const deviceId = data.deviceId || 'unknown';
    const timestamp = data.timestamp || Date.now();
    const readings = data.readings || {};

    try {
      db.exec('BEGIN');
      for (const [sensorType, reading] of Object.entries(readings)) {
        const alertInfo = state.checkThreshold(sensorType, reading.value);
        const th = alertInfo.threshold;
        state.insertSensorStmt.run(
          deviceId, sensorType, reading.value, reading.unit,
          alertInfo.alert ? 1 : 0, alertInfo.direction,
          th ? th.min : null, th ? th.max : null, timestamp
        );
        if (realtime) {
          state.processAlertTransition(deviceId, sensorType, reading.value, alertInfo, timestamp);
          state.latestData[sensorType] = {
            value: reading.value,
            unit: reading.unit,
            name: reading.name,
            timestamp,
            alert: alertInfo.alert,
            alertDirection: alertInfo.direction
          };
        }
      }
      if (realtime) {
        state.lastSeen[deviceId] = timestamp;
      }
      db.exec('COMMIT');
      return true;
    } catch (e) {
      try { db.exec('ROLLBACK'); } catch (_) {}
      console.error('[offline] process payload failed:', e.message);
      return false;
    }
  }

  function ingest(data) {
    const deviceId = data.deviceId || 'unknown';
    const timestamp = data.timestamp || Date.now();
    const fp = makeFingerprint(deviceId, timestamp);
    const payload = JSON.stringify(data);
    const now = Date.now();

    if (isAlreadyDone(fp)) return { dedup: true };

    const info = state.enqueueStmt.run(deviceId, payload, timestamp, fp, now);
    const wasInserted = info.changes > 0;

    if (!wasInserted) {
      const row = state.fetchOneStmt.get(fp);
      if (row && row.status === 'done') return { dedup: true };
    }

    const ok = processPayloadInternal(data);
    if (ok) {
      state.markDoneStmt.run(now, fp);
      state.lastSuccessTs = now;
    } else {
      state.markAttemptStmt.run(now, MAX_RETRY, fp);
      state.lastFailedTs = now;
    }

    refreshPendingCount();
    return { dedup: false, processed: ok };
  }

  function flushPendingBatch() {
    if (state.processing) return;
    state.processing = true;

    try {
      const rows = state.fetchPendingStmt.all(RETRY_BATCH);
      if (rows.length === 0) {
        refreshPendingCount();
        state.processing = false;
        return;
      }

      let success = 0;
      let failed = 0;

      for (const row of rows) {
        const now = Date.now();
        try {
          const data = JSON.parse(row.payload);
          const ok = processPayloadInternal(data, { realtime: false });
          if (ok) {
            state.markDoneStmt.run(now, row.fingerprint);
            state.lastSuccessTs = now;
            success++;
          } else {
            state.markAttemptStmt.run(now, MAX_RETRY, row.fingerprint);
            state.lastFailedTs = now;
            failed++;
          }
        } catch (e) {
          state.markAttemptStmt.run(now, MAX_RETRY, row.fingerprint);
          state.lastFailedTs = now;
          failed++;
        }
      }

      refreshPendingCount();
      if (success || failed) {
        console.log(`[offline] flush batch: success=${success}, failed=${failed}, pending=${state.pendingCount}`);
      }
    } catch (e) {
      console.error('[offline] flush error:', e.message);
    } finally {
      state.processing = false;
    }
  }

  function start() {
    prepareStatements();
    refreshPendingCount();

    aedes.on('client', () => {
      state.brokerConnected = true;
      console.log('[offline] broker: client connected, marking broker up');
    });
    aedes.on('clientDisconnect', () => {
      const connectedClients = Object.keys(aedes.clients).length;
      if (connectedClients === 0) {
        console.log('[offline] broker: all clients disconnected');
      }
    });

    if (state.retryTimer) clearInterval(state.retryTimer);
    state.retryTimer = setInterval(flushPendingBatch, RETRY_INTERVAL_MS);
    state.retryTimer.unref && state.retryTimer.unref();

    console.log('[offline] 离线补传队列已启动, 当前待补传:', state.pendingCount);
  }

  function stop() {
    if (state.retryTimer) {
      clearInterval(state.retryTimer);
      state.retryTimer = null;
    }
  }

  function getHealthSnapshot() {
    refreshPendingCount();
    const clients = aedes ? Object.keys(aedes.clients || {}).length : 0;
    return {
      brokerConnected: state.brokerConnected || clients > 0,
      brokerClients: clients,
      pendingQueueDepth: state.pendingCount,
      lastRetransmitTs: state.lastSuccessTs || null,
      lastFailedTs: state.lastFailedTs || null,
      processedInSession: 0
    };
  }

  const router = Router();

  router.get('/api/health', (req, res) => {
    try {
      const snap = getHealthSnapshot();
      res.json({
        success: true,
        data: {
          broker: {
            connected: snap.brokerConnected,
            connectedClients: snap.brokerClients
          },
          queue: {
            pending: snap.pendingQueueDepth,
            lastSuccess: snap.lastRetransmitTs,
            lastFailed: snap.lastFailedTs
          },
          schemaVersion: schemaVersion || 0,
          timestamp: Date.now()
        }
      });
    } catch (e) {
      console.error('[health] error:', e.message);
      helpers.sendError(res, 500, '健康检查失败', 'HEALTH_FAILED');
    }
  });

  return {
    start, stop, ingest, getHealthSnapshot, router,
    setSharedRefs, makeFingerprint, state
  };
};
