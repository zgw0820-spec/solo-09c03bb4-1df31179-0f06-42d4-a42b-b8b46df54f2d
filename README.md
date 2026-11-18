# Device Fault Log Ops — 在线建索引 + 可靠写入平台

设备故障日志持续写入，运维通过 API 在线构建 `(device_id, occurred_at)` 索引（`CREATE INDEX CONCURRENTLY`），构建期间日志照常写入。两个无状态 API 实例通过 PostgreSQL 自身协调，保证同一索引的并发申请只启动一次构建。

网关在 **PostgreSQL 短时故障** 时仍可提交故障日志：可靠写入接口把记录**持久暂存**到两实例共用的卷上，数据库恢复后自动补写，值班员恢复后直接在原有查询接口中读到这些记录。

## 架构

```
                ┌────────────┐        ┌────────────┐
   客户端 ─────▶│  api1      │        │  api2      │◀───── 客户端
   (:8001/docs) │ FastAPI    │        │ FastAPI    │ (:8002/docs)
                └──┬──────┬──┘        └──┬──────┬──┘
                   │      │   同一个数据库 │      │
                   │      └───────┬────────┘      │
                   │              ▼               │
                   │   ┌─────────────────────┐    │
                   │   │  PostgreSQL 16 (db) │    │
                   │   │  device_fault_logs  │    │  request_id 唯一索引
                   │   │  index_build_jobs   │    │
                   │   └─────────────────────┘    │
                   │              ▲               │
                   └──────┬───────┴───────────────┘
                          ▼
                ┌───────────────────┐
                │ staging 卷(共用)   │  故障期记录持久暂存；
                │ /staging          │  两实例的后台补写线程都扫描它
                └───────────────────┘
```

## 可靠写入（request_id 幂等 + 故障暂存）

`POST /logs/reliable` 的受理顺序：

1. **固定 `occurred_at`**：客户端未提供时，在受理时刻取 UTC 当前时间并随记录固化；补写时原样入库，**绝不改为恢复时刻**。
2. **先尝试入库**：`INSERT ... ON CONFLICT (request_id) DO NOTHING`（`uq_fault_logs_request_id` 唯一索引兜底）。成功 → `201 persisted`。
3. **库不可用则持久暂存**：写临时文件 → fsync → 原子硬链接为最终名（`O_EXCL` 语义，绝不覆盖）→ fsync 目录。全部完成才回 `202 staged`。
4. **两处都失败 → `503`**，记录未受理，客户端应重试。

幂等语义（同号 = `request_id`）：

| 场景 | 结果 |
| --- | --- |
| 同号同内容并发提交（跨实例） | 唯一索引/原子链接收敛，**只入库一次**；仅一个请求是实际写入，其余 `deduplicated: true` |
| 回执丢失后重试同内容 | 命中去重，返回原记录（含原受理时刻），不重复入库 |
| 同号不同内容 | `409 conflict`（库可达时严格校验；库不可达时对暂存目录严格校验） |
| 入库成功但暂存文件未清理 | 状态接口以库为准报 `persisted`（**不误报待入库**），总览列入 `awaiting_cleanup`；补写线程按同内容去重后删文件，**不重复写入** |
| 暂存记录与库中同号记录内容冲突 | 补写不覆盖、不无限重试，隔离为 `<request_id>.json.conflict.<ts>` 并在状态/总览接口可见 |

“内容”按客户端提供的字段判定（`device_id`/`severity`/`message`，以及**仅当客户端显式提供时**的 `occurred_at`）：省略 `occurred_at` 的回执重试会命中原记录并取回原受理时刻，而不是被误判为冲突。

恢复与接续：

- 每个实例都有后台补写线程（`BACKFILL_INTERVAL_SECONDS`，默认 2s），扫描共用暂存目录，库一恢复即自动补写；补写后的记录立即出现在原有 `GET /logs` 查询中。
- 暂存目录是 **Docker Compose 命名卷**（两实例共同挂载），实例重启、容器重建都丢不掉；实例启动即开始扫描，接续补写。
- 两实例并发补写无需加锁：插入幂等（唯一索引）、删文件幂等（忽略不存在）。
- 实例在数据库故障期间重启时，启动流程会等待 schema 就绪后才对外服务（与原有行为一致）——故障窗口内由另一台实例承接写入。

## 在线建索引协调

全部落在数据库内，实例无状态、无领导者：

| 需求 | 机制 |
| --- | --- |
| 并发申请只构建一次 | `INSERT ... ON CONFLICT (index_name) DO NOTHING`（唯一约束保证跨实例只有一个赢家）；临界区再套一层会话级 advisory lock |
| 构建期间日志照常写入 | `CREATE INDEX CONCURRENTLY`（不阻塞 DML），构建在独立后台线程 + 独立连接上执行 |
| 构建中的无效目录项不得误删 | 清理前检查 `pg_stat_progress_create_index` / `pg_stat_activity` 是否有活跃构建；有则绝不动无效索引；新认领的任务在宽限期内同样受保护 |
| 确认有效才报告就绪 | 构建完成后校验 `pg_index.indisvalid && indisready` 且 `pg_get_indexdef` 定义匹配 `(device_id, occurred_at)`，才置 `ready`；`GET /indexes/status` 每次也实时核对目录 |
| 失败/崩溃后清理重试 | 宽限期后无活跃构建的 `building` 记录判定为孤儿 → `failed`；重试时先 `DROP INDEX CONCURRENTLY IF EXISTS` 清掉无效残件再重新认领 |
| 重启状态一致 | 实例启动时 `reconcile()`：把台账与 `pg_catalog` 实际状态对齐（孤儿→failed、已完成→ready、目录矛盾→failed） |
| 迟到旧作业不得覆盖新结果 | 所有终态写入带围栏条件 `WHERE id = <job> AND status = 'building'`；任务行被清理重建后旧作业的 UPDATE 命中 0 行 |

已知限制：实例崩溃后，其 `building` 记录需等 `ORPHAN_GRACE_SECONDS`（默认 20s，compose 中设 15s）宽限期过后才被判定为孤儿——这是区分“认领后尚未在 `pg_stat_activity` 可见”与“构建者已死”的必要窗口。

## 运行（Docker Compose）

```bash
docker compose up -d --build
# Swagger UI: http://localhost:8001/docs  或  http://localhost:8002/docs
# PostgreSQL: localhost:5433 (ops/ops, db=faultlogs)
```

首次启动时 `db/init.sql` 预置 400 万行日志（约需几十秒），db 健康检查会等到 schema 就绪后才放行 API 容器。`staging` 命名卷由 api1/api2 共同挂载，是故障期记录的唯一暂存处。

## API

| 方法/路径 | 说明 |
| --- | --- |
| `POST /logs/reliable` | **可靠写入**：`request_id` 必填（幂等键）。`201 persisted`（已入库）/ `202 staged`（已持久暂存待补写）/ `409`（同号不同内容）/ `503`（库与暂存都失败，未受理） |
| `GET /logs/reliable/{request_id}` | 单条状态：`persisted`（库中，原查询可见）/ `staged`（待补写）/ `conflicted`（暂存与库冲突，已隔离）/ `not_found`；库不可达且无暂存副本时 `503 unknown` |
| `GET /logs/reliable` | 暂存队列总览：`pending`（真待补写）与 `awaiting_cleanup`（已入库的残留副本，**不算待办**）分列，冲突/损坏清单，补写线程心跳 |
| `POST /logs` | 普通写入（库正常时照常可用） |
| `GET /logs` | 按 `device_id` + 时间范围查询（走索引；补写的记录同样从这里读出） |
| `GET /logs/plan` | `EXPLAIN` 上述查询，验证索引被使用 |
| `POST /indexes/build` | 幂等申请建索引；并发调用只会有一个 `started`，其余 `building`；已就绪返回 `ready` |
| `GET /indexes/status` | 任务状态 + 实时目录核对 |
| `POST /indexes/cleanup` | 确认无活跃构建后，删除无效残件索引并清理失败任务，允许重试 |
| `GET /health` | 健康检查 |

`POST /indexes/build` 返回的 `state`：`started`（本次认领成功）、`building`（他处已在构建）、`ready`（目录已确认有效）、`conflict`（同名索引定义不符，需人工处理）。

## 自测

`tests/selftest.py` 端到端验证（P 系列为建索引，R 系列为可靠写入）：

- P：并发去重、构建期写入、无效目录项保护、崩溃→孤儿检测→清理→重试、迟到作业围栏、重启一致性、查询计划命中索引。
- R：库正常时可靠写入与回执重试去重、同号不同内容 409、并发同号只入一行；**断库**（local 模式 SIGSTOP 冻结 postgres / docker 模式 `compose stop db`）期间 `202` 持久暂存、受理时刻固化、恢复后自动补写并在原查询接口可读；双实例硬重启接续补写；“已入库+暂存残留”不误报待办且不重复写；冲突暂存隔离；库与暂存同时失败 `503`；显式 `occurred_at` 全程不变。

```bash
# Docker 模式（需要本机有 docker）：
docker compose up -d --build
python3 tests/selftest.py --mode docker \
  --dsn "postgresql://ops:ops@localhost:5433/faultlogs"

# 本地模式（无 docker，使用 pgserver 提供的 PostgreSQL 二进制）：
python3 -m venv /tmp/ops-venv && /tmp/ops-venv/bin/pip install -r requirements.txt pgserver
tests/run_local.sh
```

## 验证状态

`tests/selftest.py` 已对两个真实 API 实例 + 真实 PostgreSQL 16.2（400 万行预置数据）完整跑通，**113/113 检查通过**（P 系列 64 项 + R 系列 49 项），覆盖：

- P：并发申请去重（12 并发仅 1 次构建、活跃构建后端数恒为 1）、构建期写入零失败、构建中的无效目录项全程未被删除、就绪前目录校验、崩溃后服务端完成构建的自愈、构建被中断后的无效残件清理与重试、迟到旧作业围栏（UPDATE 命中 0 行）、启动 reconcile 与重启一致性、查询计划命中索引。
- R：库正常时可靠写入 201、回执丢失重试去重（同 id 同行）、同号不同内容 409 且不改库；12 路并发同号同内容仅 1 次实际插入、并发同号不同内容一边全 201 一边全 409 且库中仅 1 行；断库（SIGSTOP 冻结 postgres）期间 202 持久暂存、暂存期重复提交幂等、不同内容 409、状态/总览可查；恢复后自动补写、`occurred_at` 保持受理时刻（精确到微秒，非恢复时刻）、原 `GET /logs` 可读、暂存文件清理；双实例硬重启接续补写；“已入库+暂存残留”状态报 persisted 从不误报 pending、补写去重删文件不重复写；冲突暂存隔离且库行不变；断库+暂存目录不可写时 503、恢复后重试成功；显式 `occurred_at` 经暂存-补写全程不变。

说明：本交付环境的沙箱没有 Docker，`docker-compose.yml`/`Dockerfile` 未实际构建运行（已做 YAML 语法校验）；上述验证以 local 模式（pgserver 提供的 PostgreSQL 16.2 + 两个本地 uvicorn 实例）完成，应用代码路径与 docker 模式完全一致。local 模式的断库用 `pkill -STOP/CONT postgres` 模拟，docker 模式对应 `docker compose stop/start db`（该分支未实际运行）。

## 目录

```
app/main.py           FastAPI 接口（普通日志、可靠写入、状态、建索引）
app/reliable.py       可靠写入核心：受理、持久暂存、补写、状态查询
app/index_manager.py  建索引协调核心（认领/围栏/清理/核对/重启对齐）
app/db.py             连接（含连接超时）、schema 等待、幂等迁移
db/init.sql           建表 + request_id 唯一索引 + 400 万行预置 + 任务台账表
tests/selftest.py     端到端自测（local/docker 两种编排，P+R 系列）
tests/local_db.py     本地 pgserver 数据库引导
docker-compose.yml    db(postgres:16, shm_size=256m) + api1 + api2 + staging 卷
```
