# 机构知识库论文变更同步系统

把机构知识库（源）中论文的**新增 / 修订 / 撤回**同步到检索库（采集端），
保证撤回论文不再被检出。FastAPI + SQLite 实现，`docker compose` 一键启动，
内置一个**无需账号的本地知识库源**，可演示**迟到撤回、取页期间事件追加、
修订去重、撤回粘性、断线续采与游标过期**。

## 为什么需要到达水位（arrival watermark）

仅按业务变更时间 `changed_at` 做增量窗口会漏掉**迟到事件**：一条撤回的
`changed_at` 落在上次已完成的旧区间里，但它在该次同步结束之后才到达源端，
纯时间窗口的下一次任务永远看不到它，论文于是一直可被检索。

本系统给每条被接收的变更在入库时分配一个与业务时间无关、**单调递增的到达
序列号 `seq`**，源端全局水位 `watermark = max(seq)`。采集端同时维护两条基线：

- `completed_at`：本次闭区间结束时间（业务时间基线）；
- `watermark_seq`：已发布的到达水位（到达基线）。

任务的采集谓词为

```
changed_at <= end AND (changed_at >= start OR seq > after_seq)
```

即**原闭区间**与**水位之后到达、但业务时间落在旧区间的迟到事件**的并集；
业务时间晚于本次截至时间的事件仍被排除，等后续任务采集。

## 目录结构

```
.
├── docker-compose.yml        # source + collector 两个服务，SQLite 持久化到命名卷
├── source/                   # 本地知识库源（无鉴权，seq 水位 + 快照分页 + 故障注入）
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── data.py           # 不可改写的变更事件种子（含乱序、重复、旧版重投、迟到批次）
│       └── main.py           # /changes 变更流、/watermark、/control/* 演示控制、/events
├── collector/                # 采集端：采集 API、暂存、原子发布、可查询论文库
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── db.py             # SQLite 表结构、暂存去重、撤回粘性、原子发布（含水位）
│       ├── sync.py           # 快照分页、after_seq 补取、每页检查点、410 重取、worker
│       ├── schemas.py
│       └── main.py           # 采集 / 续采 / 查询 HTTP API
├── tests/                    # pytest（httpx MockTransport，无需起服务）
├── demo/demo.sh              # 端到端演示脚本
└── requirements-dev.txt      # 本地测试依赖
```

## 快速开始

```bash
docker compose up -d --build
./demo/demo.sh                 # 全流程演示（也可只用下面的 curl 手动调用）
```

- 源服务： http://localhost:8000 （OpenAPI 文档 `/docs`）
- 采集端： http://localhost:8001 （OpenAPI 文档 `/docs`）

## 核心语义如何落地

| 需求 | 实现方式 |
| --- | --- |
| 源端为已接收变更提供单调到达水位 | 事件入库即在锁内分配递增 `seq`；`GET /watermark` 返回 `max(seq)`，事件与 `/changes` 响应均携带 `seq` |
| 原时间区间 + 迟到事件并集采集 | `/changes` 支持 `after_seq`；谓词 `changed_at<=end AND (changed_at>=start OR seq>after_seq)`，任务 `after_seq` 取上次发布水位 |
| 分页固定同一源快照 | 无游标请求把当前水位钉为 `snapshot_seq` 并写入签名游标；续页只看 `seq<=snapshot_seq` 的事件，取页期间新增对本次 feed 不可见 |
| 取页期间新增不被游标/提交时序永久跳过 | 发布的水位取**本次快照**而非源端实时水位；新增事件 seq 大于快照，下一次任务的 `after_seq` 窗口必然采到 |
| 尚未到本次截至时间的事件留给后续任务 | `changed_at > end` 一律排除（与 seq 无关）；后续截止时间覆盖后经闭区间重取、修订去重入账 |
| 输入源地址 + 同步截至时间 | `POST /sync/jobs`，body `{"source_url": "...", "end": "...", "start": "..."}`；`start` 可省略 |
| 首次全采，之后从上次完成时间起采 | 无 `completed_at` 时 `start` 取纪元起点；之后默认取上次区间 `end`。闭区间边界事件重取并按修订号去重 |
| 跨页乱序重复、旧版重投 | 暂存按 `paper_id`  UPSERT，**仅保留最高 `rev`**，同修订号重复用 `MAX(is_retracted)` 兜底 |
| 撤回不能被旧版覆盖 | 暂存表与正式表的 UPSERT 都以 `excluded.rev > 现有 rev` 为更新条件；旧修订改不掉字段也改不掉撤回位（**撤回粘性**） |
| 每页变更与续采游标一同持久化 | 每拉到一页，在**同一事务**写 `staged_changes` 并更新 `jobs.cursor / next_offset / pages_fetched / snapshot_seq` |
| 游标失效沿原任务续采 | 过期/越界/`after_seq` 不符的游标返回 `410`；采集端清空本任务暂存、游标归零、`cursor_resets+1`，**保留原区间与 after_seq** 重取 |
| 进程中断后续采 | 重启时 `collecting/committing` 任务翻转为 `interrupted`，可 `resume`；检查点在 SQLite 命名卷上不丢 |
| 取齐后一同发布论文库、完成时间、到达水位 | feed 完成后在**单事务**内发布暂存到 `papers`、写 `sync_state.completed_at=end` 与 `watermark_seq=snapshot_seq`；失败整体回滚 |
| 失败时旧库可查、暂存不可见 | `staged_changes` 与 `papers` 物理隔离；查询 API 只读 `papers`，未发布暂存对外完全不可见 |
| 撤回论文不被检出 | `GET /papers` 默认隐藏 `is_retracted=1`；`GET /papers/{id}` 对撤回论文返回 **HTTP 410 Gone**（`?include_retracted=true` 可审计） |

> `completed_at` 记录本次**区间结束时间**（下次拉取的业务时间基线），
> `watermark_seq` 记录本次**钉住快照的到达水位**（迟到事件基线），
> 二者都不是墙上时钟，回填历史区间时也不会让增量起点错位。

## 采集端 API

| 方法与路径 | 说明 |
| --- | --- |
| `POST /sync/jobs` | 创建同步任务。body：`end`（必填）、`start`（可选，默认上次完成时间/全采）、`source_url`（可选） |
| `GET /sync/jobs` | 任务列表（含检查点：页数、偏移、游标、重置次数、`after_seq`、`snapshot_seq`、暂存数、错误） |
| `GET /sync/jobs/{id}` | 单任务状态 |
| `POST /sync/jobs/{id}/resume` | 续采 `failed/interrupted` 任务，沿用原闭区间、after_seq 与持久化游标 |
| `GET /sync/state` | 上次同步的源、区间、完成时间与到达水位 `watermark_seq` |
| `GET /papers` | 论文列表，默认隐藏撤回；`?include_retracted=true` 可包含 |
| `GET /papers/{id}` | 按编号查询；撤回论文返回 410 |
| `GET /papers-count` | 计数 |

任务状态机：`collecting → committing → completed`；异常进入 `failed`，
进程在途崩溃在重启时变为 `interrupted`，二者均可 `resume`。
同一时刻只允许一个未完成任务（重叠区间返回 409）。

## 源服务 API（演示用，采集端不调用控制接口）

| 方法与路径 | 说明 |
| --- | --- |
| `GET /watermark` | 当前单调到达水位 `max(seq)` |
| `GET /changes?start&end[&after_seq][&cursor]` | 闭区间 + 到达水位补取的分页变更流；响应含 `snapshot_seq` 与当前 `watermark`；游标带 HMAC 签名、TTL 与代次 |
| `GET /events` | 查看全部事件（含 seq） |
| `POST /control/fail` `{"times":N}` | 接下来 N 次 `/changes` 返回 503；`times:0` 清除 |
| `POST /control/expire-after` `{"requests":N}` | 第 N 次成功响应后作废旧游标（确定性游标过期演示） |
| `POST /control/expire-cursors` | 立即作废旧游标（手动） |
| `POST /control/append-batch` | 追加预置增量批次（新论文、再修订、撤回 + 旧版重投） |
| `POST /control/append-late-batch` | 追加预置**迟到批次**：撤回的 changed_at 在旧区间、seq 为最新 |
| `POST /control/append` | 追加自定义事件（seq 由源端分配） |
| `POST /control/append-during-paging` | 让事件在下一次 `/changes` 响应之后入库（取页期间追加演示） |
| `POST /control/reset` / `GET /control/state` | 重置 / 查看源状态 |

## 演示脚本章节

`./demo/demo.sh` 依次演示：

1. 首次全采（分页、乱序/重复去重、撤回隐藏、最高修订胜出）；
2. 503 断线后沿检查点续采，失败期间旧馆藏可查、暂存不可见；
3. **迟到撤回**：P0004 的撤回 `changed_at=2026-11-04` 却在 11-11 才到达，
   经 `after_seq` 补取后论文返回 410；
4. **取页期间追加**：D1/D2 在分页响应之间入库，被快照挡在当前 feed 外，
   发布水位停在快照，下一任务经水位窗口补齐，不被跳过；
5. 游标过期后清空暂存、按**同一区间与 after_seq** 重取；
6. 采集端进程重启，在途任务变 `interrupted` 后续采完成。

## 手动演示要点

```bash
# 1) 首次全采（撤回的 P0001/P0003 自动隐藏）
curl -X POST localhost:8001/sync/jobs -H 'Content-Type: application/json' \
  -d '{"end":"2026-11-08T23:59:59Z"}'
curl localhost:8001/papers/P0001            # 410 Gone

# 2) 迟到撤回：changed_at 在旧区间，靠到达水位补取
curl -X POST localhost:8000/control/append-late-batch
curl -X POST localhost:8001/sync/jobs -H 'Content-Type: application/json' \
  -d '{"end":"2026-11-11T23:59:59Z"}'
curl -s -o /dev/null -w '%{http_code}\n' localhost:8001/papers/P0004   # 410

# 3) 取页期间到达的事件不会被跳过
curl -X POST localhost:8000/control/append-during-paging -H 'Content-Type: application/json' \
  -d '{"events":[{"paper_id":"D1","rev":1,"title":"t","authors":"a","is_retracted":0,"changed_at":"2026-11-12T08:00:00Z"}]}'
curl -X POST localhost:8001/sync/jobs -H 'Content-Type: application/json' \
  -d '{"end":"2026-11-12T23:59:59Z"}'        # D1 不在本 feed 快照内
curl -X POST localhost:8001/sync/jobs -H 'Content-Type: application/json' \
  -d '{"end":"2026-11-13T23:59:59Z"}'        # 下一任务经 after_seq 采到 D1
```

## 本地测试（不依赖 Docker）

```bash
pip install -r requirements-dev.txt
PYTHONPATH=. python -m pytest -q
```

覆盖：分页乱序/重复去重、最高修订胜出、撤回粘性与 410、失败时旧库可查且
暂存不可见、续采、游标过期重取原 feed（区间与 after_seq 不变）、重启后
interrupted 翻转、**迟到撤回经水位补取**、**取页期间事件经快照隔离 +
下一任务水位补齐**、**超截止时间事件留待后续任务**、水位单调不降、
源端 seq/水位单调性、快照钉扎与游标绑定 after_seq 等。

## 数据安全与隔离说明

- 采集端 SQLite（WAL 模式）放在命名卷 `collector_data`，容器重启不丢检查点。
- 源端事件为 append-only 不可改写；到达 seq 单调分配，采集端不修改源数据。
- 所有“对外可见”的只有 `papers` 表；`staged_changes`、`jobs`、`sync_state`
  仅服务于同步过程与运维观测。
