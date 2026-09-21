# 设备遥测接收 API（device-telemetry-ingest）

基于 **FastAPI + SQLite** 的本地设备遥测接收服务。设备按批次上报带递增序列号的遥测记录，批次可能乱序到达；服务为每台设备维护一个**滑动窗口**，实现乱序暂存、重复幂等丢弃、超时断档标记与断档重放，并在并发请求下保持状态一致。此外支持**连续阈值告警**：按设备创建规则，监视 payload 顶层数值字段，连续若干条 committed 记录越过阈值时生成一条可查询告警。

## 快速启动

### Docker Compose（推荐）

```bash
docker compose up --build
```

### 本地运行

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### 启动后

| 项目 | 地址 |
|---|---|
| API 基础地址 | `http://localhost:8000/api/v1` |
| 交互式文档 (Swagger) | `http://localhost:8000/docs` |
| 健康检查 | `http://localhost:8000/health` |
| 服务信息/当前配置 | `http://localhost:8000/` |
| 端口 | `8000`（Compose 映射 `8000:8000`，可在 `docker-compose.yml` 修改） |

SQLite 数据库文件默认在容器内 `/data/telemetry.db`，Compose 通过命名卷 `telemetry-data` 持久化。

## 配置项（环境变量）

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `TELEMETRY_DB_PATH` | `/data/telemetry.db` | SQLite 数据库文件路径 |
| `TELEMETRY_WINDOW_SIZE` | `1000` | 每设备滑动窗口大小：接受 `[next_expected, next_expected + window_size)` 内的序列号 |
| `TELEMETRY_GAP_TIMEOUT_SECONDS` | `30` | 缺口等待时间（秒），超时标记为断档 |
| `TELEMETRY_SWEEP_INTERVAL_SECONDS` | `5` | 后台扫描间隔（秒），无流量时也会判定断档 |
| `TELEMETRY_SWEEP_ENABLED` | `true` | 是否启用后台扫描（测试时可关闭，断档判定退化为访问时惰性触发） |
| `TELEMETRY_CURSOR_SECRET` | `telemetry-cursor-dev-secret` | 分页游标 HMAC 签名密钥；生产环境务必改为强随机值（`docker-compose.yml` 支持同名 shell 变量覆盖） |

## 核心语义

每台设备维护 `next_expected`（下一个期望的连续序列号），接收窗口为
`[next_expected, next_expected + window_size)`。设备首次出现时，以该批最小序列号锚定窗口起点。

| 到达序列号位置 | 处理 | 返回状态 |
|---|---|---|
| `seq == next_expected` | 落库为已提交，窗口前移，并连续冲刷已暂存的记录 | `committed` |
| 窗口内、`next_expected` 之后 | 暂存，等待前方缺口补齐 | `buffered` |
| 已存在相同序列号 | 幂等丢弃，不重复落库（批次整体重试安全） | `duplicate` |
| 窗口之下、属于已声明断档 | 作为补录数据填充断档 | `gap_filled` |
| 窗口之下、无记录、无断档、距窗口 ≤ window_size 且从未断档 | 窗口回锚（并发首批竞争导致锚点偏高时向下修正）后正常接收 | `committed`/`buffered` |
| 窗口之下、其余情况 | 过旧数据，忽略 | `stale` |
| 超出窗口上界 | 拒绝，客户端应在窗口前移后重试 | `out_of_window` |

**断档（gap）**：`next_expected` 与最小暂存序列号之间存在缺口，且持续超过
`TELEMETRY_GAP_TIMEOUT_SECONDS`，即标记断档：写入 `gaps` 表（状态 `open`），
`next_expected` 跳过缺口，暂存记录继续冲刷。断档由后台扫描器周期性判定，也会在每次请求时惰性判定。

**断档重放**：客户端查询断档 → 调用重放接口（状态转 `replaying`，返回缺失序列号区间）
→ 设备通过正常批次接口重发缺失序列号 → 逐条落为 `gap_filled` → 缺口全部补齐后断档自动转为 `recovered`。

**并发一致性**：每请求一个 SQLite 连接（WAL 模式 + `busy_timeout`），所有状态变更包裹在
`BEGIN IMMEDIATE` 写事务中，写操作在数据库层面串行化；同步端点运行在线程池中，
配合每序列号幂等，并发/重试/乱序下状态保持一致。

## 连续阈值告警

每台设备可创建若干条规则，规则指定 **payload 顶层数值字段**、比较方向（`gt`/`lt`）、
**阈值**与**连续条数**。规则按 seq 顺序推进，计数与触发语义：

- **仅在记录首次转为 committed 时推进**：顺序提交的记录立即计入；先前 `buffered` 的记录
  在窗口前移冲刷为 committed 时才计入。一次提交冲刷多条 buffered 记录时，**逐条**按 seq
  升序计算（跨批次也连续）。内部以每设备「评估前沿」（`alert_seq`）推进：记录严格按
  seq 升序、每条恰好评估一次，与并发下各事务的实际提交先后无关；乱序竞争中晚到的记录
  会在其前驱补齐后才被走到。
- **重复不重算**：重复批次落为 `duplicate`，不参与计数；同一连续区段不会因重试产生多条告警。
- **断档中断**：声明断档（超时跳过缺口）时连续计数立即清零，评估前沿跳过缺口，冲刷过缺口的
  buffered 记录从 1 开始组成新区段；**断档补录**（`gap_filled`）不参与推进，不回溯、不恢复旧区段。
- **并发首批锚点竞争**：首个 committed 区段以当时的最低 committed seq 为起点；极端并发下，
  更短 seq 的记录随后经窗口「回锚」修正补提交时不属于断档，不会重置连续计数，也不回头重算
  （它们物理上晚于前方区段到达）。正常单客户端顺序/乱序上报不受影响。
- **字段异常中断**：payload 顶层缺少该字段，或字段不是数值（字符串、布尔、`null`、
  嵌套对象等），连续计数清零。
- **恢复正常后可再触发**：字段存在、数值正常且不违反阈值的记录结束当前区段（重新武装）；
  之后连续违反再次达到条数时才生成下一条告警。
- **首次达到条数生成一条告警**：区段内只在计数第一次等于连续条数的记录上生成一条告警，
  区段随后继续违反也不重复生成。`consecutive_count=1` 时每个违反区段生成一条。

规则创建后只对**此后**新 committed 的记录生效，不回溯历史数据。

## API 一览

### 提交批次

```
POST /api/v1/batches
```

```json
{
  "device_id": "dev-001",
  "batch_id": "optional-client-id",
  "records": [
    {"seq": 1, "payload": {"temp": 21.5}},
    {"seq": 2, "payload": {"temp": 21.6}}
  ]
}
```

响应（每条记录独立返回处理结果，HTTP 恒为 200，除非请求体校验失败）：

```json
{
  "device_id": "dev-001",
  "batch_id": "optional-client-id",
  "accepted": 2,
  "results": [{"seq": 1, "status": "committed"}, {"seq": 2, "status": "buffered"}],
  "window": {"next_expected": 2, "buffered_count": 1}
}
```

### 查询窗口状态

```
GET /api/v1/devices/{device_id}/window
```

返回 `next_expected`、窗口范围、暂存序列号列表、待判定缺口（含剩余等待时间）、
已声明断档列表与统计计数（committed/buffered/gap_filled/duplicates）。

### 一致性分页查询记录

```
GET /api/v1/devices/{device_id}/records?seq_from=1&seq_to=100&limit=50
GET /api/v1/devices/{device_id}/records?cursor=<next_cursor>
```

按 `seq` 升序做**键集分页**（keyset pagination），参数：

| 参数 | 说明 |
|---|---|
| `seq_from` / `seq_to` | 可选，序列号闭区间过滤（仅首页请求需要传入） |
| `limit` | 可选，每页条数，默认 100，最大 1000 |
| `cursor` | 可选，上一页响应返回的 `next_cursor`；传入后区间与页大小以游标为准 |

**一致性语义**：首次请求固定一个 SQLite rowid 高水位（snapshot 边界），并随不透明
`next_cursor` 一起返回；后续页沿用该高水位与查询区间。翻页期间到达的新批次、断档补录
（`gap_filled`）的 rowid 都大于高水位，因此**不会混入**本次记录集合；已存在的记录原地
更新（如 `buffered` → `committed`）不改变 rowid，**不会漏读**；键集翻页保证每条记录
**恰好返回一次**。集合成员固定，但记录字段（如 `status`）反映读取时的最新值。

**游标安全**：游标为 base64url(JSON) + HMAC-SHA256 签名，密钥取自
`TELEMETRY_CURSOR_SECRET`。以下情况均返回 **400**：签名不符或格式非法（篡改）、
跨设备复用、复用时更改 `seq_from`/`seq_to`/`limit`。与游标一致的重复参数允许传入。
未知设备返回 404；`seq_from > seq_to` 返回 400。

响应示例（无下一页时 `next_cursor` 为 `null`）：

```json
{
  "device_id": "dev-001",
  "records": [
    {"seq": 1, "payload": {"temp": 21.5}, "status": "committed", "received_at": 1758000000.12}
  ],
  "next_cursor": "eyJ2IjoxLC...9oZg.4f3b2a...9c"
}
```

### 查询断档

```
GET /api/v1/devices/{device_id}/gaps?status=open
```

`status` 可选过滤：`open` / `replaying` / `recovered`。

### 断档重放

```
POST /api/v1/devices/{device_id}/gaps/{gap_id}/replay
```

将断档标记为 `replaying` 并返回缺失区间，例如：

```json
{
  "gap": {"gap_id": 1, "start_seq": 4, "end_seq": 6, "status": "replaying", "...": "..."},
  "missing_ranges": [[4, 6]],
  "missing_count": 3
}
```

设备随后经 `POST /api/v1/batches` 重发缺失序列号即可补齐。

### 创建连续阈值告警规则

```
POST /api/v1/devices/{device_id}/alert-rules
```

请求体（`field` 仅允许 payload **顶层**字段名；`op` 为 `gt` 或 `lt`；
`consecutive_count ≥ 1`；阈值必须为有限数值，非法请求返回 422）：

```json
{
  "field": "temp",
  "op": "gt",
  "threshold": 40.0,
  "consecutive_count": 3
}
```

成功返回 **201**：

```json
{
  "rule_id": 1,
  "device_id": "dev-001",
  "field": "temp",
  "op": "gt",
  "threshold": 40.0,
  "consecutive_count": 3,
  "created_at": 1758000000.12
}
```

规则可在设备上报数据之前创建（不回溯历史），也可对同一设备/字段创建多条规则，
各规则独立计数。

### 查询规则

```
GET /api/v1/devices/{device_id}/alert-rules   # 某设备的规则
GET /api/v1/alert-rules                        # 全部设备的规则
```

响应：

```json
{"device_id": "dev-001", "rules": [{"rule_id": 1, "...": "..."}]}
```

### 查询告警

```
GET /api/v1/alerts
GET /api/v1/devices/{device_id}/alerts
```

可选查询参数：

| 参数 | 说明 |
|---|---|
| `rule_id` | 仅返回指定规则产生的告警（可与 `device_id` 组合） |
| `limit` | 每页条数，默认 100，最大 1000，按告警 id 升序返回 |
| `after_id` | 键集轮询参数：只返回 id 大于该值的告警（传已见最大 alert_id） |

响应（`trigger_seq` 为首次达到连续条数的记录，`run_start_seq` 为该区段起点，
`value` 为触发记录中的字段值）：

```json
{
  "alerts": [
    {
      "alert_id": 1,
      "device_id": "dev-001",
      "rule_id": 1,
      "field": "temp",
      "op": "gt",
      "threshold": 40.0,
      "consecutive_count": 3,
      "trigger_seq": 6,
      "run_start_seq": 4,
      "value": 43.2,
      "created_at": 1758000010.55
    }
  ]
}
```

### 其他

- `GET /api/v1/devices` — 设备列表
- `GET /health` — 健康检查
- `GET /` — 服务信息与当前配置

## 完整流程示例

```bash
# 1. 乱序提交：1,2,3,5,6（缺口 4）
curl -X POST localhost:8000/api/v1/batches -H 'Content-Type: application/json' -d '{
  "device_id": "dev-001",
  "records": [{"seq":1,"payload":{}},{"seq":2,"payload":{}},{"seq":3,"payload":{}},
              {"seq":5,"payload":{}},{"seq":6,"payload":{}}]
}'
# -> 1,2,3 committed；5,6 buffered

# 2. 查询窗口：pending_gap 显示缺口 4 及剩余等待时间
curl localhost:8000/api/v1/devices/dev-001/window

# 3. 超过 GAP_TIMEOUT 后，断档被标记（后台扫描或下次访问时）
curl localhost:8000/api/v1/devices/dev-001/gaps
# -> gap_id=1, start=4, end=4, status=open

# 4. 请求重放
curl -X POST localhost:8000/api/v1/devices/dev-001/gaps/1/replay
# -> status=replaying, missing_ranges=[[4,4]]

# 5. 设备重发 seq 4
curl -X POST localhost:8000/api/v1/batches -H 'Content-Type: application/json' -d '{
  "device_id": "dev-001", "records": [{"seq":4,"payload":{"temp":21.7}}]
}'
# -> status=gap_filled；断档自动 recovered

# 6. 一致性分页拉取记录（第一页，固定高水位快照）
curl 'localhost:8000/api/v1/devices/dev-001/records?seq_from=1&seq_to=100&limit=2'
# -> {"records": [...seq 1, 2...], "next_cursor": "eyJ2Ijox..."}

# 7. 后续页只带 cursor；期间新到的批次/补录不会混入本次结果
curl 'localhost:8000/api/v1/devices/dev-001/records?cursor=eyJ2Ijox...'
# -> 翻页直至 next_cursor 为 null，区间内每条记录恰好返回一次
```

## 连续阈值告警示例

```bash
# 1. 创建规则：temp > 40 连续 3 条即告警
curl -X POST localhost:8000/api/v1/devices/dev-001/alert-rules \
  -H 'Content-Type: application/json' \
  -d '{"field":"temp","op":"gt","threshold":40,"consecutive_count":3}'
# -> 201 {"rule_id":1,...}

# 2. 乱序上报：4 先 buffered（缺口 3），此时计数为 2，不告警
curl -X POST localhost:8000/api/v1/batches -H 'Content-Type: application/json' -d '{
  "device_id":"dev-001",
  "records":[{"seq":1,"payload":{"temp":41}},
             {"seq":2,"payload":{"temp":42}},
             {"seq":4,"payload":{"temp":44}}]
}'

# 3. 补齐 seq 3：3 提交并按 seq 顺序冲刷 4，计数 1,2,3 在 seq 3 首次达到 -> 一条告警
curl -X POST localhost:8000/api/v1/batches -H 'Content-Type: application/json' -d '{
  "device_id":"dev-001",
  "records":[{"seq":3,"payload":{"temp":43}}]
}'

# 4. 重试整批：全部 duplicate，不会产生第二条告警
curl -X POST localhost:8000/api/v1/batches -H 'Content-Type: application/json' -d '{
  "device_id":"dev-001",
  "records":[{"seq":1,"payload":{"temp":41}},{"seq":2,"payload":{"temp":42}},
             {"seq":3,"payload":{"temp":43}},{"seq":4,"payload":{"temp":44}}]
}'

# 5. 查询告警（同区段只有一条；恢复正常后再次连续越限才会出现下一条）
curl localhost:8000/api/v1/devices/dev-001/alerts
# -> {"device_id":"dev-001","alerts":[{"alert_id":1,"trigger_seq":3,
#     "run_start_seq":1,"value":43.0,...}]}

# 6. 轮询增量告警
curl 'localhost:8000/api/v1/alerts?device_id=dev-001&after_id=1&limit=100'
```

## 项目结构

```
app/
  config.py   # 环境变量配置
  db.py       # SQLite 连接、建表、写事务（WAL + BEGIN IMMEDIATE）
  window.py   # 滑动窗口核心引擎（提交/暂存/断档/重放/回锚）
  alerts.py   # 连续阈值告警引擎（规则推进/断档重置/区段去重/告警查询）
  cursor.py   # HMAC 签名的不透明分页游标（高水位 + 键集位置）
  models.py   # 请求/响应模型
  main.py     # FastAPI 应用、路由、后台断档扫描器
tests/
  test_api.py     # 乱序/幂等/断档/重放/回锚/并发一致性测试
  test_records.py # 一致性分页：快照隔离/游标篡改/跨设备/区间变更测试
  test_alerts.py  # 连续阈值告警：触发/去重/中断/冲刷逐条/断档重置/规则接口测试
Dockerfile
docker-compose.yml
requirements.txt / requirements-dev.txt
```

## 运行测试

```bash
pip install -r requirements-dev.txt
pytest tests/ -q
```

测试覆盖：顺序提交、乱序暂存与冲刷、重复幂等（含批内重复）、窗口外拒绝、
断档超时标记、重放恢复（含多序列号部分补齐）、窗口回锚、设备自举，
8 线程并发提交 200 条记录后的状态一致性校验（真实 uvicorn 服务），
一致性分页：多页不重不漏、翻页期间新批次/断档补录不混入、原地状态变更不漏读、
游标篡改/跨设备/更改区间复用均返回 400，
以及连续阈值告警：首次达到条数触发、同区段不重复、恢复正常后再武装、
字段缺失/非数值中断、buffered 冲刷逐条按 seq 计算（含同批多条与跨批）、
重复批次不重复计数、断档声明清零且补录不回溯、多设备/多规则独立、规则与告警查询接口。
