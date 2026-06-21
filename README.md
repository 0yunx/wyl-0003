# 智能温室 IoT 边缘网关模拟器

一个完整的智能温室 IoT 边缘网关模拟系统，包含传感器模拟器、MQTT 消息推送、数据存储、REST API 和实时监控前端。

## 功能特性

- **5 路传感器模拟**：温度、湿度、光照、土壤湿度、CO₂
- **MQTT 协议**：内置 Aedes MQTT Broker，每 2 秒推送 JSON 数据
- **数据持久化**：SQLite 存储历史数据 + 告警事件，断电重启不丢失
- **Schema 版本管理**：内置 migration 机制，换机器/升版本自动升级数据库结构
- **REST API**：统一响应格式（`{ success, data }` / `{ success, error }`），无堆栈泄露
- **流式数据导出**：`/api/export` 支持百万级时间窗 CSV/JSON 流式导出，可选 GZIP 压缩，浏览器下载不 OOM
- **离线补传**：模拟器断连时数据入本地持久化队列，重连后自动补发；服务端按 `(device_id, timestamp, sensor_type)` 唯一索引去重，`/api/health` 暴露 broker 状态与补传队列深度
- **模拟器信号注入**：定期随机触发 socket hangup 或进程卡死（5-8s），验证补传链路健壮性
- **实时监控**：Chart.js 折线图，2 秒刷新，含导出面板与离线队列面板
- **阈值告警**：后端落库告警事件，前端曲线变红、数值闪烁、告警横幅、事件列表
- **告警状态机**：触发/恢复/设备离线/超时离线/服务关闭，5 种状态完整流转
- **离线检测双重保障**：MQTT 遗嘱消息 + 后端定时轮询（10 秒超时判离线）
- **热更新配置**：阈值可通过 API 动态调整
- **前端性能优化**：告警列表增量 DOM 更新，避免全量重绘和重排
- **版本锁定**：依赖精确版本号，无 `^` 前缀，避免 break change

## 文件结构

```
wyl-0003/
├── server.js          # Express 服务 + MQTT Broker + SQLite 存储 + migration
├── simulator.js       # 5 路传感器 MQTT 模拟器（含信号注入 + 本地补传队列）
├── export.js          # 流式数据导出路由（/api/export）
├── offline_queue.js   # 离线补传队列管理（/api/health + 定时 flush）
├── public/
│   └── index.html     # Chart.js 实时监控前端（含导出面板 + 离线队列面板）
├── db.sqlite          # SQLite 数据库（自动生成）
├── package.json
└── README.md
```

## 快速开始

### 安装依赖

```bash
npm install
```

### 启动服务

方式一：分别启动（推荐用于调试）

```bash
# 终端 1：启动服务端
npm start

# 终端 2：启动模拟器
npm run simulator
```

方式二：一键启动（同时运行服务和模拟器）

```bash
npm run dev
```

### 访问前端

打开浏览器访问：http://localhost:3000

## API 接口

### GET /api/latest

获取最新传感器数据和设备状态。

**响应示例：**
```json
{
  "deviceId": "greenhouse-edge-001",
  "online": true,
  "lastSeen": 1719000000000,
  "sensors": {
    "temperature": { "value": 25.5, "unit": "°C", "name": "温度", "alert": false },
    "humidity": { "value": 55.2, "unit": "%", "name": "湿度", "alert": false }
  },
  "thresholds": {
    "temperature": { "min": 10, "max": 35 }
  }
}
```

### GET /api/history

查询历史数据。每条记录包含写入时的告警标记（`alert`、`alert_direction`、`threshold_min`、`threshold_max`），均为写入时刻的真实状态，不是前端事后重算。

**参数：**
- `sensor` - 传感器类型（可选，如 temperature）
- `from` - 起始时间戳（毫秒）
- `to` - 结束时间戳（毫秒）
- `limit` - 返回数量限制

**示例：**
```
GET /api/history?sensor=temperature&from=1719000000000&to=1719003600000
```

### GET /api/alerts

查询告警事件列表。

**参数：**
- `sensor` - 传感器类型（可选）
- `status` - 告警状态：`active` / `resolved` / `device_offline` / `shutdown`
- `from` - 起始时间戳（毫秒）
- `to` - 结束时间戳（毫秒）
- `limit` - 返回数量限制

**事件状态说明：**
- `active` — 告警进行中
- `resolved` — 已恢复正常
- `device_offline` — 因设备离线结束
- `shutdown` — 因服务关闭结束

### GET /api/config

获取当前阈值配置。

### POST /api/config

更新阈值配置（热更新）。

**请求体：**
```json
{
  "thresholds": {
    "temperature": { "min": 15, "max": 30 }
  }
}
```

### GET /api/status

获取设备在线状态。

## MQTT 主题

| 主题 | 说明 | QoS |
|------|------|-----|
| `greenhouse/sensors` | 传感器数据推送 | 0 |
| `greenhouse/status` | 设备状态（保留消息） | 1 |

**传感器数据格式：**
```json
{
  "deviceId": "greenhouse-edge-001",
  "timestamp": 1719000000000,
  "readings": {
    "temperature": { "value": 25.5, "unit": "°C", "name": "温度" },
    "humidity": { "value": 55.2, "unit": "%", "name": "湿度" },
    "light": { "value": 30000, "unit": "lux", "name": "光照" },
    "soil": { "value": 60.5, "unit": "%", "name": "土壤湿度" },
    "co2": { "value": 800, "unit": "ppm", "name": "CO₂" }
  }
}
```

## 离线补传架构

```
模拟器 (simulator.js)                 服务端 (server.js + offline_queue.js)
┌─────────────────────┐              ┌──────────────────────────────────────┐
│  定时推送 sensor 数据  │── MQTT ───▶│  ingest()                            │
│                     │              │  ├─ 计算 fingerprint (SHA1)           │
│  信号注入机制：       │              │  ├─ INSERT OR IGNORE → offline_queue  │
│  ├─ socket hangup   │              │  ├─ INSERT OR IGNORE → sensor_data   │
│  └─ 进程卡死 5-8s   │              │  └─ markDone / markAttempt            │
│                     │              │                                      │
│  断连时：            │              │  定时 flush (3s)：                    │
│  ├─ 数据入本地队列    │              │  ├─ 拉取 pending 行                  │
│  ├─ 落盘 .json 文件  │              │  ├─ 重试写入 sensor_data             │
│  └─ 重连后批量补发    │── MQTT ───▶│  └─ 成功则 markDone                   │
│                     │              │                                      │
└─────────────────────┘              │  /api/health 暴露：                   │
                                     │  ├─ broker 连接状态                   │
                                     │  ├─ 补传队列 pending 深度              │
                                     │  └─ 最近补传时间戳                     │
                                     └──────────────────────────────────────┘
```

**去重保证**：`sensor_data` 表有 `UNIQUE(device_id, timestamp, sensor_type)` 索引，`INSERT OR IGNORE` 保证同一帧数据不会重复写入；`offline_queue` 表有 `UNIQUE(fingerprint)` 索引，fingerprint 由 `SHA1(deviceId|timestamp)` 生成，保证同一帧不会重复入队。

## 验证步骤

1. **启动服务**：运行 `npm start` 和 `npm run simulator`
2. **查看实时曲线**：浏览器打开 http://localhost:3000，应看到 5 条实时折线
3. **测试离线检测**：手动 kill 模拟器进程（Ctrl+C），10 秒后页面显示"设备离线"
4. **测试阈值告警**：在前端配置面板将温度最大值调低（如 20），曲线变红并触发告警横幅
5. **测试数据持久化**：重启服务端，历史曲线数据仍然存在

## 默认阈值

| 传感器 | 最小值 | 最大值 |
|--------|--------|--------|
| 温度 (°C) | 10 | 35 |
| 湿度 (%) | 30 | 70 |
| 光照 (lux) | 5000 | 60000 |
| 土壤湿度 (%) | 30 | 80 |
| CO₂ (ppm) | 400 | 1500 |

## 环境变量

- `PORT` - HTTP 服务端口，默认 3000
- `MQTT_PORT` - MQTT Broker 端口，默认 1883
- `MQTT_BROKER` - 模拟器连接的 MQTT Broker 地址，默认 mqtt://localhost:1883

## Schema 版本迁移

项目内置 migration 机制，数据库 schema 版本自动升级，换机器或升级版本无需手动处理。

| 版本 | 变更内容 |
|------|----------|
| v1 | 初始版本：sensor_data 表、config 表 |
| v2 | 新增 alert 相关字段 + alerts 告警事件表 |
| v3 | schema_version 版本记录表 |

启动时自动检测当前版本并执行未应用的迁移，迁移记录保存在 `schema_version` 表中。

## 技术栈

- **后端**：Node.js + Express
- **MQTT Broker**：Aedes
- **数据库**：Node.js 内置 SQLite (node:sqlite)
- **前端**：Chart.js + 原生 HTML/CSS/JS
