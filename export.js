const { Router } = require('express');
const zlib = require('zlib');
const { Readable, Transform } = require('stream');

const EXPORT_BATCH = 500;
const EXPORT_MAX_WINDOW_MS = 365 * 24 * 60 * 60 * 1000;

function createCsvEscapeStream() {
  return new Transform({
    writableObjectMode: true,
    transform(row, _enc, cb) {
      const escaped = row.map(v => {
        if (v === null || v === undefined) return '';
        const s = String(v);
        if (s.includes(',') || s.includes('"') || s.includes('\n') || s.includes('\r')) {
          return '"' + s.replace(/"/g, '""') + '"';
        }
        return s;
      });
      cb(null, escaped.join(',') + '\r\n');
    }
  });
}

function createJsonArrayStream(totalHint) {
  let first = true;
  let count = 0;
  return new Transform({
    writableObjectMode: true,
    transform(row, _enc, cb) {
      let prefix = '';
      if (first) {
        prefix = '{"success":true,"data":{"count":' + totalHint + ',"items":[';
        first = false;
      } else {
        prefix = ',';
      }
      count++;
      cb(null, prefix + JSON.stringify(row));
    },
    flush(cb) {
      let suffix = '';
      if (first) {
        suffix = '{"success":true,"data":{"count":' + totalHint + ',"items":[]}}';
      } else {
        suffix = ']}}';
      }
      this.push(suffix);
      cb();
    }
  });
}

function buildCountQuery(from, to, sensor) {
  let sql = 'SELECT COUNT(*) as c FROM sensor_data WHERE 1=1';
  const params = [];
  if (sensor) { sql += ' AND sensor_type = ?'; params.push(sensor); }
  if (from)   { sql += ' AND timestamp >= ?'; params.push(parseInt(from)); }
  if (to)     { sql += ' AND timestamp <= ?'; params.push(parseInt(to)); }
  return { sql, params };
}

function buildSelectQuery(from, to, sensor) {
  let sql = `SELECT id, device_id, sensor_type, value, unit,
    CASE WHEN alert = 1 THEN 'true' ELSE 'false' END as alert,
    alert_direction, threshold_min, threshold_max, timestamp
    FROM sensor_data WHERE 1=1`;
  const params = [];
  if (sensor) { sql += ' AND sensor_type = ?'; params.push(sensor); }
  if (from)   { sql += ' AND timestamp >= ?'; params.push(parseInt(from)); }
  if (to)     { sql += ' AND timestamp <= ?'; params.push(parseInt(to)); }
  sql += ' ORDER BY timestamp ASC, id ASC LIMIT ? OFFSET ?';
  return { sql, params };
}

function createCursorStream(db, from, to, sensor, batchSize) {
  const { sql: countSql, params: countParams } = buildCountQuery(from, to, sensor);
  const totalRow = db.prepare(countSql).get(...countParams);
  const total = totalRow ? totalRow.c : 0;

  let offset = 0;
  let exhausted = false;
  let stmt = null;

  return new Readable({
    objectMode: true,
    highWaterMark: batchSize * 2,
    read() {
      if (exhausted) { this.push(null); return; }
      if (!stmt) stmt = db.prepare(buildSelectQuery(from, to, sensor).sql);

      const batchParams = [...buildSelectQuery(from, to, sensor).params, batchSize, offset];
      const rows = stmt.all(...batchParams);

      if (rows.length === 0) {
        exhausted = true;
        this.push(null);
        return;
      }

      offset += rows.length;
      for (let i = 0; i < rows.length; i++) {
        if (!this.push(rows[i])) {
          if (i < rows.length - 1) {
            process.nextTick(() => {
              for (let j = i + 1; j < rows.length; j++) this.push(rows[j]);
            });
          }
          return;
        }
      }

      if (rows.length < batchSize) {
        exhausted = true;
        process.nextTick(() => this.push(null));
      }
    },
    destroy(err, cb) {
      try { if (stmt) stmt.free && stmt.free(); } catch (_) {}
      cb(err);
    }
  });

  Object.defineProperty(module, 'exports_total', { value: total, configurable: true });
}

function attachTotal(total) {
  return new Transform({
    writableObjectMode: true,
    readableObjectMode: true,
    transform(row, _enc, cb) {
      if (!this._totalAttached) {
        this._totalAttached = true;
        this.emit('total', total);
      }
      cb(null, row);
    }
  });
}

module.exports = function createExportRouter(db, helpers) {
  const router = Router();

  router.get('/api/export', (req, res) => {
    const { from, to, format = 'csv', compress = 'none', sensor } = req.query;

    const fmt = String(format).toLowerCase();
    const cmp = String(compress).toLowerCase();

    if (fmt !== 'csv' && fmt !== 'json') {
      return helpers.sendError(res, 400, 'format 必须是 csv 或 json', 'INVALID_FORMAT');
    }
    if (cmp !== 'none' && cmp !== 'gzip') {
      return helpers.sendError(res, 400, 'compress 必须是 none 或 gzip', 'INVALID_COMPRESS');
    }

    const fromTs = from ? parseInt(from) : null;
    const toTs = to ? parseInt(to) : null;

    if (from !== undefined && (isNaN(fromTs) || fromTs <= 0)) {
      return helpers.sendError(res, 400, 'from 必须是有效的时间戳(ms)', 'INVALID_FROM');
    }
    if (to !== undefined && (isNaN(toTs) || toTs <= 0)) {
      return helpers.sendError(res, 400, 'to 必须是有效的时间戳(ms)', 'INVALID_TO');
    }
    if (fromTs && toTs && fromTs > toTs) {
      return helpers.sendError(res, 400, 'from 不能大于 to', 'INVALID_RANGE');
    }
    if (fromTs && toTs && (toTs - fromTs) > EXPORT_MAX_WINDOW_MS) {
      return helpers.sendError(res, 400, '时间窗口超过最大 1 年限制', 'RANGE_TOO_LARGE');
    }

    const { sql: countSql, params: countParams } = buildCountQuery(from, to, sensor);
    let total;
    try {
      total = (db.prepare(countSql).get(...countParams) || {}).c || 0;
    } catch (e) {
      console.error('[export] count error:', e.message);
      return helpers.sendError(res, 500, '数据统计失败', 'COUNT_FAILED');
    }

    res.setHeader('X-Total-Count', String(total));
    res.setHeader('Accept-Ranges', 'none');

    if (req.method === 'HEAD') {
      return res.status(200).end();
    }

    const safeFrom = fromTs || '';
    const safeTo = toTs || '';
    const baseName = `greenhouse_export_${safeFrom}_${safeTo}.${fmt}`;
    const fileName = cmp === 'gzip' ? baseName + '.gz' : baseName;

    res.setHeader('Content-Disposition', `attachment; filename="${fileName}"`);
    res.setHeader('X-Total-Count', String(total));
    res.setHeader('Accept-Ranges', 'none');
    if (cmp === 'gzip') {
      res.setHeader('Content-Encoding', 'gzip');
    }

    let contentType;
    if (fmt === 'csv') {
      contentType = cmp === 'gzip' ? 'application/gzip' : 'text/csv; charset=utf-8';
    } else {
      contentType = cmp === 'gzip' ? 'application/gzip' : 'application/json; charset=utf-8';
    }
    res.setHeader('Content-Type', contentType);

    const cursor = createCursorStreamInternal(db, from, to, sensor, EXPORT_BATCH);

    let formatter;
    const pipelineStreams = [cursor];
    if (fmt === 'csv') {
      res.write('\ufeff');
      const header = 'id,device_id,sensor_type,value,unit,alert,alert_direction,threshold_min,threshold_max,timestamp\r\n';
      res.write(header);
      pipelineStreams.push(createObjectToArrayStream());
      pipelineStreams.push(createCsvEscapeStream());
    } else {
      pipelineStreams.push(createJsonArrayStream(total));
    }

    if (cmp === 'gzip') {
      const gzip = zlib.createGzip({ level: zlib.constants.Z_BEST_SPEED });
      pipelineStreams.push(gzip);
    }
    pipelineStreams.push(res);

    let aborted = false;
    req.on('aborted', () => { aborted = true; });
    res.on('close', () => { aborted = true; });

    function pipeChain(streams) {
      for (let i = 0; i < streams.length - 1; i++) {
        const src = streams[i];
        const dst = streams[i + 1];
        src.on('error', (e) => {
          if (aborted) return;
          console.error('[export] stream error:', e.message);
          try { cursor.destroy && cursor.destroy(); } catch (_) {}
          if (!res.headersSent) res.status(500);
          try { res.end(); } catch (_) {}
        });
        src.pipe(dst);
      }
    }
    pipeChain(pipelineStreams);
  });

  return router;
};

function createObjectToArrayStream() {
  const cols = ['id','device_id','sensor_type','value','unit','alert','alert_direction','threshold_min','threshold_max','timestamp'];
  return new Transform({
    writableObjectMode: true,
    readableObjectMode: true,
    transform(obj, _enc, cb) {
      cb(null, cols.map(c => obj[c]));
    }
  });
}

function createCursorStreamInternal(db, from, to, sensor, batchSize) {
  let offset = 0;
  let exhausted = false;
  let stmt = null;
  const { sql: selectSql, params: baseParams } = buildSelectQuery(from, to, sensor);

  return new Readable({
    objectMode: true,
    highWaterMark: batchSize * 2,
    read() {
      if (exhausted) { this.push(null); return; }
      if (!stmt) stmt = db.prepare(selectSql);

      const params = [...baseParams, batchSize, offset];
      const rows = stmt.all(...params);

      if (rows.length === 0) {
        exhausted = true;
        this.push(null);
        return;
      }

      offset += rows.length;
      for (let i = 0; i < rows.length; i++) {
        this.push(rows[i]);
      }

      if (rows.length < batchSize) {
        exhausted = true;
        process.nextTick(() => this.push(null));
      }
    },
    destroy(err, cb) {
      try { if (stmt && stmt.free) stmt.free(); } catch (_) {}
      cb(err);
    }
  });
}
