# InferScale v1.0 技术设计文档

**项目定位**：面向多 GPU / 多模型副本场景的高性能 LLM Serving 基础设施  
**版本**：v1.0（秋招版范围修订，2026-09-22）  
**核心方向**：Replica-level Routing / Scheduling、流式服务、可靠性、可观测性、Benchmark  
**推理后端**：vLLM  
**服务语言**：Python 3.12  
**部署目标**：单机/单台 Linux GPU 服务器上的两个独立 Replica；Kubernetes 为后续扩展

---

## 0. 当前实施范围（2026-09-22 修订）

本项目按秋招作品交付：一个 Gateway 进程、一个模型、两个同构独立 Replica、单信任域、四种路由、三类主实验。当前 P0–P3 已验收，GPU 取消由用户人工确认；P4 固定轨迹、实验 runner 和汇总比较已实现，157 项 CPU 测试通过。真实 GPU 性能对比和最终图表待完成。部署包说明见 `deploy/README.md`，详细状态见 `docs/代码实现规划.md`。

本节及修订后的 §8、§14–17、§20、§23、§35、§37–38、§47–48 为当前基线。完整规则见 `docs/核心正确性约定.md`，开发任务见 `docs/代码实现规划.md`。其它章节中的 Kubernetes、多租户配额、动态背压、全量监控、六类 workload 和完整实验矩阵保留为扩展参考，其“必须/至少”不再作为秋招版门槛。§50 的简历示例也只在实际完成对应能力后使用。

本轮必须解决：成本公式不改变排序、请求计数泄漏、流式重试重复输出、实验口径失真。减少功能范围，不降低这四项的正确性。

---

## 1. 文档目的

InferScale 的目标不是重新实现 vLLM，而是在 vLLM 单实例推理引擎之上，构建一套完整的多 Replica LLM Serving 平台。

项目重点解决：

1. 多个模型 Replica 之间如何进行请求路由。
2. 如何感知 Worker 的队列、KV Cache、请求成本与健康状态。
3. 如何在 Load Balance 和 Prefix Cache Locality 之间做权衡。
4. 如何处理高并发、背压、限流、取消、超时和 Worker 故障。
5. 如何建立端到端 LLM Serving 可观测性。
6. 如何通过可重复 Benchmark 证明不同路由策略的收益与局限。
7. 如何将系统从本地多进程部署扩展到 Kubernetes。

项目最终必须能够回答一个问题：

> 在模型相同、硬件相同的情况下，为什么 InferScale 的调度策略在某些 workload 下比简单 Round Robin 更合适，以及代价是什么？

---

## 2. 项目边界

### 2.1 InferScale 自己实现

- OpenAI-compatible Gateway
- API Key / Tenant / Rate Limit
- Request Validation
- Streaming Proxy
- Admission Control
- Backpressure
- Worker Registry
- Worker Health / Heartbeat
- Replica-level Request Scheduler
- Round Robin Routing
- Least Load Routing
- Cost-Aware Routing
- Prefix-Aware Routing
- Request Lifecycle
- Retry / Timeout / Cancel
- Scheduler Metrics
- Benchmark Framework
- Fault Injection
- Prometheus / Grafana 集成
- Docker / Kubernetes Deployment

### 2.2 vLLM 负责

- 模型加载
- Tokenizer
- 单 Replica 内 sequence scheduling
- Continuous Batching
- Paged KV Cache
- Prefix Cache 实际存储与复用
- GPU Kernel 执行
- Tensor Parallel
- 单实例内部请求执行
- vLLM 原生 metrics

### 2.3 v1 明确不做

- 自研 CUDA Kernel
- 自研 PagedAttention
- 自研 Continuous Batching Engine
- 模型训练/微调
- RAG
- Agent
- MCP
- 向量数据库
- 多模态
- MoE Expert Parallel 优化
- Prefill/Decode Disaggregation
- 跨节点 KV Cache 迁移
- 强一致分布式 Scheduler

这些内容可以作为 v2 或第二个 Mini Inference Engine 项目的扩展，不能污染 v1 的核心目标。

---

## 3. 核心技术问题

### 3.1 为什么不能只用 Round Robin

LLM 请求不是等成本请求。

请求成本至少受到以下因素影响：

- Prompt token 数
- 实际 output token 数
- Worker 当前 running request 数
- Worker 当前 waiting request 数
- KV Cache 压力
- Prefix Cache 是否命中
- Prefill 和 Decode 所处阶段
- GPU 当前可用计算与显存资源

例如：

- 请求 A：Prompt 256 tokens，Output 64 tokens
- 请求 B：Prompt 16K tokens，Output 2K tokens

Round Robin 会把两者视为相同单位，但其 Prefill、Decode、KV Cache 占用和排队影响完全不同。

因此 InferScale 的核心研究问题是：

> 如何构建比连接数或请求数更适合 LLM workload 的 Replica-level routing policy。

### 3.2 两级调度模型

系统中存在两个不同层次的调度：

**第一层：InferScale Replica-level Scheduler**

决定：

`Request -> Replica A / Replica B / Replica C`

**第二层：vLLM Engine Scheduler**

决定同一个 Replica 内部的 sequence 如何进入当前 engine step、如何执行 batching、preemption 和 KV block 管理。

InferScale 不修改第二层。

---

## 4. 系统总体架构

```text
                                  Client
                                     |
                                     | OpenAI-compatible HTTP/SSE
                                     v
                         +------------------------+
                         |      API Gateway       |
                         |------------------------|
                         | Auth                   |
                         | Validation             |
                         | Rate Limit             |
                         | Admission Control      |
                         | Streaming Proxy        |
                         +-----------+------------+
                                     |
                                     v
                         +------------------------+
                         |   Request Scheduler    |
                         |------------------------|
                         | Round Robin            |
                         | Least Load             |
                         | Cost Aware             |
                         | Prefix Aware           |
                         +-----------+------------+
                                     |
                 +-------------------+-------------------+
                 |                   |                   |
                 v                   v                   v
          +-------------+     +-------------+     +-------------+
          | Replica 0   |     | Replica 1   |     | Replica 2   |
          | vLLM        |     | vLLM        |     | vLLM        |
          | GPU 0       |     | GPU 1       |     | GPU 2       |
          +------+------+     +------+------+     +------+------+
                 |                   |                   |
                 +-------------------+-------------------+
                                     |
                                     v
                         +------------------------+
                         |  Worker State Manager  |
                         |------------------------|
                         | heartbeat              |
                         | queue state            |
                         | kv cache state         |
                         | health                 |
                         +------------------------+

          Prometheus <---- Gateway / Scheduler / vLLM / DCGM
               |
               v
            Grafana
```

---

## 5. 技术栈

| 层 | 技术 |
|---|---|
| Language | Python 3.12 |
| API | FastAPI |
| ASGI | Uvicorn |
| Async HTTP | httpx / aiohttp（二选一，建议 httpx） |
| Validation | Pydantic v2 |
| LLM Backend | vLLM |
| GPU Framework | PyTorch / CUDA Runtime |
| Metrics | Prometheus |
| Dashboard | Grafana |
| GPU Telemetry | NVIDIA DCGM Exporter |
| Logging | structlog 或 Python logging JSON formatter |
| Tracing | OpenTelemetry |
| Testing | pytest / pytest-asyncio |
| Load Test | 自研 asyncio benchmark client |
| Container | Docker |
| Orchestration | Kubernetes |
| CI | GitHub Actions |

v1 不要求 Redis、Ray、Kafka、PostgreSQL 作为核心依赖。

---

## 6. 进程与部署模型

### 6.1 本地开发模式

```text
gateway/scheduler :8000

vllm-worker-0     :8100 GPU0
vllm-worker-1     :8101 GPU1

prometheus        :9090
grafana           :3000
```

Gateway 与 Scheduler v1 可以在同一 Python 进程内，减少网络跳数和系统复杂度。

### 6.2 Kubernetes 模式

```text
Gateway Deployment
      |
      v
Scheduler / Router
      |
      +------> vLLM Replica Pod 0 -> GPU
      +------> vLLM Replica Pod 1 -> GPU
      +------> vLLM Replica Pod 2 -> GPU
```

当模型单卡可容纳：

`1 Replica = 1 GPU`

当模型需要 TP=2：

`1 Replica = 2 GPUs`

InferScale 调度单位永远是 Replica，而不是物理 GPU。

---

## 7. API 设计

### 7.1 对外 API

v1 必须支持：

```text
POST /v1/chat/completions
POST /v1/completions
GET  /health/live
GET  /health/ready
GET  /metrics
```

### 7.2 Chat Completion 请求

核心字段保持与 OpenAI 风格一致：

```json
{
  "model": "qwen3-8b",
  "messages": [],
  "temperature": 0.7,
  "top_p": 0.9,
  "max_tokens": 512,
  "stream": true
}
```

内部额外生成：

```text
request_id
tenant_id
arrival_at
prompt_token_count
estimated_output_tokens
prefix_fingerprint
routing_policy
selected_worker
```

### 7.3 响应 Header

建议加入调试 Header，仅在 benchmark / debug 模式开启：

```text
X-InferScale-Request-ID
X-InferScale-Worker-ID
X-InferScale-Routing-Policy
X-InferScale-Queue-Estimate
```

生产模式可关闭，防止暴露内部拓扑。

---

## 8. 请求生命周期与计数所有权

请求与上游执行分开：`request_id` 表示一次用户请求；`attempt_id` 表示一次上游执行。一个 request 至多顺序执行两个 attempt，重试不增加逻辑请求数。

```text
RECEIVED -> VALIDATED -> ADMITTED -> EXECUTING -> COMPLETED
                                      |
                                      +-> TIMEOUT / CANCELLED / WORKER_FAILED / INTERNAL_ERROR
RECEIVED / VALIDATED -> REJECTED

attempt: RESERVED -> FORWARDED -> PREFETCHING -> COMMIT_STARTED -> STREAMING -> FINISHED
                        |             |
                        +-------------+-> FAILED（尚未提交时才可能新建 attempt）
```

非流式成功不必经过 STREAMING；合法无文本流也可完成。`commit_started` 在尝试发送响应头前置位；首有效内容时间单独记录。

### 8.1 唯一账本

`active_requests[request_id]` 保存 request lease；`reservations[attempt_id]` 保存 request、Worker/epoch 和分配时成本。计数与成本从字典派生，避免在各层零散加减。

- 检查容量并插入 lease 原子执行；过滤、选择并写入 reservation 原子执行；锁内不 await。
- request 控制器拥有全部 attempt；attempt 拥有 upstream response。所有路径进入统一 finally，关闭上游、幂等删除 reservation/lease，逻辑 outcome 只记一次。
- A 失败后，先清理 A 的 attempt，再为 B 预占；request lease 始终只有一份。
- release 用 `pop(id, None)`；采样不能覆盖账本，远端 running/waiting 不重复加入本地计数。
- close 失败/超时仍回收本地账本，并把旧 Worker 隔离为 SUSPECT，待后端负载/实例状态确认后恢复。网关回收不等于 GPU 推理已停止。

### 8.2 时间与验收

内部 deadline/耗时使用 monotonic clock。记录入站、校验、准入、每 attempt 转发、首有效内容、提交尝试与终态时间。Gateway 的上游首输出等待包含网络/队列/prefill，不声称仅靠它拆出准确后端排队。

必须覆盖正常、连接失败、预取取消、转发取消、重试、重复 finalize、close 超时和并发容量竞争；详细验收 L1–L6 见 `docs/核心正确性约定.md`。

---

## 9. 核心数据结构

### 9.1 WorkerState

```python
@dataclass
class WorkerState:
    worker_id: str
    endpoint: str
    model: str

    health: WorkerHealth
    lifecycle: WorkerLifecycle

    running_requests: int
    waiting_requests: int

    kv_cache_usage: float

    prefix_cache_queries: int
    prefix_cache_hits: int

    gpu_utilization: float | None
    gpu_memory_used_bytes: int | None
    gpu_memory_total_bytes: int | None

    last_heartbeat_at: float
    last_metrics_at: float

    ewma_ttft_ms: float | None
    ewma_tpot_ms: float | None
```

### 9.2 RequestContext

```python
@dataclass
class RequestContext:
    request_id: str
    tenant_id: str
    model: str

    prompt_tokens: int
    max_output_tokens: int
    estimated_output_tokens: int

    prefix_fingerprint: str | None

    stream: bool

    received_at: float
    deadline_at: float | None
```

### 9.3 RoutingDecision

```python
@dataclass
class RoutingDecision:
    request_id: str
    worker_id: str
    policy: str

    score: float | None

    queue_score: float | None
    cost_score: float | None
    kv_pressure_score: float | None
    prefix_affinity_score: float | None

    decided_at: float
```

保留子分数的原因：Benchmark 后可以解释“为什么这次路由到某个 Worker”，而不是只能看到最终结果。

---

## 10. Worker Registry

### 10.1 职责

Worker Registry 保存当前所有可调度 Replica 的 soft state。

```python
class WorkerRegistry:
    async def register(worker)
    async def update(worker_id, metrics)
    async def mark_unhealthy(worker_id, reason)
    async def list_schedulable(model) -> list[WorkerState]
    async def remove(worker_id)
```

### 10.2 为什么使用内存

Worker runtime state：

- 更新频率高
- 生命周期短
- 可以重建
- 位于请求关键路径

因此 v1 使用 Scheduler 本地内存保存，而不是每次通过 Redis 查询。

Scheduler 重启后通过：

- 静态配置 / Kubernetes Service Discovery
- Worker health probe
- vLLM `/metrics`

重新构建状态。

这是 soft state，不要求持久化。

---

## 11. Worker State Collector

Worker State Collector 每个采样周期获取：

```text
running requests
waiting requests
kv cache usage
prefix cache hits
prefix cache queries
TTFT histogram
TPOT / ITL
request failures
GPU utilization
GPU memory
```

数据来源分三类：

### 11.1 vLLM Metrics

通过 `/metrics` 获取。

### 11.2 Infra Metrics

通过 DCGM Exporter / Prometheus 获取 GPU 数据。

### 11.3 Gateway Runtime Metrics

Gateway 自己维护：

```text
active streams
in-flight requests
routing decisions
retry count
cancel count
```

### 11.4 热路径原则

路由时只读 Registry 中最近一次采样状态，不同步请求 Prometheus。

错误设计：

```text
Request
 -> Scheduler
 -> Prometheus query
 -> select worker
```

正确设计：

```text
Background Collector -> WorkerRegistry

Request -> Scheduler -> WorkerRegistry(memory)
```

---

## 12. RoutingPolicy 抽象

```python
class RoutingPolicy(Protocol):
    async def select_worker(
        self,
        request: RequestContext,
        workers: list[WorkerState],
    ) -> RoutingDecision: ...
```

要求所有 Policy：

1. 输入相同。
2. 输出结构相同。
3. 可以通过配置动态切换。
4. 可以在 benchmark 中严格 A/B。
5. Policy 不直接发送 HTTP 请求。

---

## 13. Policy 1：Round Robin

### 13.1 目的

Benchmark baseline。

### 13.2 算法

对满足以下条件的 Worker：

```text
health == HEALTHY
lifecycle == ACTIVE
model matches
```

循环选择。

### 13.3 局限

不考虑：

- 请求长度
- Worker queue
- KV Cache
- Prefix locality
- GPU memory pressure

必须保留作为 baseline；是否作为最终推荐策略由具体 workload 的实验结果决定。

---

## 14. Policy 2：Least Load（秋招版）

在与 RR 相同的健康/容量候选门槛下，选择本地未完成 attempt 数占容量比例最低的 Worker：

```text
q_i = local_active_attempts_i / request_capacity_i
```

同分轮转。选择与预占原子完成，后续请求立刻看到更新。远端 running/waiting 用于观测及容量保护，不把同一批请求再加一遍。

原 running/waiting 加权方案作为后续实验扩展；首版用清晰的请求数 baseline，与 §15 的工作量账本作单一增量比较。

---

## 15. Policy 3：Cost-Aware Routing（修订）

旧公式给每个 Worker 加上相同 request_cost，不会改变同构 Worker 排序。首版改为比较各 Worker 已分配、尚未完成的估计工作量。

```text
estimated_output_r = min(output_cap_r, EWMA[model, prompt_length_bucket])
cost_r = p * prompt_tokens_r + d * estimated_output_r
C_i = sum(cost of unfinished attempt reservations on Worker i)
q_i = local_active_attempts_i / request_capacity_i
score_i = wq * q_i + wc * (C_i + cost_r) / cost_reference
```

无历史预测时使用配置默认值。`cost_reference > 0` 是同一模型池固定尺度；p/d 非负且不同时为 0，wc>0。首版不混入 KV/GPU 利用率权重；健康/容量用于共同候选门槛，KV 只观察。

同构池下当前请求的 cost_r 仍不会改变本次排序。收益来自 C_i 的历史成本差异，并且 select+reserve 立即更新 C_i，影响后续请求；这是未完成工作量启发式，不是精确剩余 GPU 时间。

示例：A/B 各有 2 个请求、容量均为 4，C_A=9000、C_B=1000，新成本=1000，cost_reference=10000，wq=wc=1。得分 A=1.5、B=0.7，选择 B；B 预占后 C=2000，完成后按原成本释放回 1000。

reservation 保存分配时成本，预测更新不能改变释放金额。仅完整成功且 usage 可信时更新 EWMA；失败、取消、无 usage 不训练预测。触及 max_tokens 的样本标记截断，不冒充自然输出长度。

prompt 数与 fingerprint 使用同版本 tokenizer/chat template；bucket 仅取到达时已知特征，不使用未来实际输出或 workload 标签。成本到终态前不递减，明确承认对接近结束的长请求可能高估。验收 C1–C5 见 `docs/核心正确性约定.md`。

---

## 16. Policy 4：Prefix-Aware Routing

### 16.1 目标

共享长 Prefix workload 下，尽量把请求送到已经缓存相关 Prefix 的 Replica，从而减少重复 Prefill。

### 16.2 Prefix Fingerprint

v1 不复制完整 Prompt。

计算：

```text
canonical prefix tokens
        |
        v
block-aligned hash / fingerprint
```

第一阶段可以定义一个应用侧近似：

```text
stable_hash(model_identity + first_N_actual_template_token_ids)
```

用于构建 affinity。

注意：

> InferScale 的 fingerprint 只是路由提示，不等同于 vLLM 内部真实 KV block hash。

不能宣称两个 hash 完全一致。

### 16.3 PrefixAffinityTable

```python
@dataclass
class PrefixAffinityEntry:
    prefix_fingerprint: str
    workers: dict[str, PrefixWorkerStat]
    expires_at: float
```

记录：

```text
prefix -> 最近成功处理过该 prefix 的 Worker
```

来源可以是：

1. InferScale 根据历史请求近似学习；
2. 后续 v2 接入更精确的 KV cache event。

### 16.4 Prefix-aware Score

```text
score_prefix_i = score_cost_i - gamma * prefix_affinity_i
# score_cost_i 严格复用 §15；prefix_affinity_i 在 [0, 1]
```

选择最小 score。

### 16.5 热点保护

不能只按 prefix affinity。

如果：

```text
Worker A:
  affinity = high
  waiting = 100

Worker B:
  affinity = none
  waiting = 0
```

系统必须允许流量去 B。

因此先过滤已达容量门槛的 Worker，再评分；prefix affinity 不能抵消硬容量限制。

---

## 17. PrefixAffinity 的更新策略

请求正常完成后（仅收到首输出还不算成功）：

```text
prefix -> selected worker
```

增加 affinity。每项记录 Worker instance epoch，使用 TTL + LRU + 最大条目数限制；gamma=0 时必须与 Cost-Aware 选择相同。

请求失败：

不增加。

Worker unhealthy：

该 Worker 的 affinity 标记失效。

设置 TTL，例如：

```text
60 ~ 300 seconds
```

具体值必须通过实验确认。

### 17.1 为什么需要 TTL

KV Cache 会被 eviction。

应用侧不能假设历史上在某个 Worker 运行过，就永远存在缓存。

---

## 18. Admission Control

Admission Control 在 Scheduler 前执行。

### 18.1 限制项

```text
max_global_inflight_requests
max_tenant_inflight_requests
max_prompt_tokens
max_output_tokens
max_total_tokens
max_request_body_bytes
max_queue_depth
```

### 18.2 拒绝策略

客户端自身超限：

```text
400 / 413 / 429
```

服务整体 overload：

```text
429 或 503
```

返回：

```text
Retry-After
```

如果能合理估算。

### 18.3 为什么不能无限排队

无限排队会：

- 推高 P95/P99 TTFT
- 增加内存占用
- 增加超时请求
- 导致 cache pressure
- 形成 retry storm

---

## 19. Backpressure

定义系统三种容量状态：

```text
NORMAL
DEGRADED
OVERLOADED
```

### NORMAL

正常接入请求。

### DEGRADED

条件示例：

```text
P95 queue delay > threshold
或
waiting requests > threshold
或
KV cache pressure > threshold
```

行为：

- 降低新并发上限
- 更积极拒绝低优先级请求

### OVERLOADED

行为：

- 快速失败
- 不继续堆积请求

v1 不做复杂优先级队列，只实现容量保护。

---

## 20. Streaming Proxy（修订）

Gateway 增量转发 SSE，不缓存完整流输出。先在响应提交前有界预取前导事件，直到首个非空文本内容或合法结束，再提交并按原顺序发送；role-only、空 delta、usage、heartbeat 不算首输出。

维护四个独立字段：

```text
commit_started       # 尝试发送 response start 之前置位，决定能否重试
response_started     # response start 的 send 成功
body_bytes_sent      # 已成功发送的 body 字节
first_output_at      # 第一条有效文本内容被网关观察到
```

发送响应头的结果可能不确定，所以 commit_started 一旦为 true 就不得重试。预取/重试在控制器或显式控制 ASGI send 的响应对象里完成，不能在已提交的 StreamingResponse 生成器中重选 Worker。

断开监听覆盖首输出等待期；请求体与断开消息由明确的 receive 所有者消费。设置单事件、前导缓存、请求体和非流式 response 大小上限；按需读取，限制下游 send 和 overall deadline。SSE parser 处理跨 chunk、同包多事件及 UTF-8 分片。

合法空输出 TTFT 为 null；非流式在完整有界 JSON 到手前不提交，也不伪造首 token 时间。提交后故障按 §23 中止；真实 socket 测试 S1–S6 是验收条件。

---

## 21. Cancellation

### 21.1 场景

- 用户关闭页面
- HTTP client disconnect
- 用户主动 cancel
- Gateway timeout

### 21.2 行为

```text
detect disconnect
      |
      v
cancel upstream request
      |
      v
vLLM abort
      |
      v
release execution/KV resources
```

### 21.3 验收

Fault / integration test 必须验证：

用户断开后，Worker 的 running request 在合理时间内下降。

---

## 22. Timeout

定义不同 timeout：

```text
connect_timeout
first_token_timeout
idle_stream_timeout
overall_deadline
```

不能只有一个 HTTP timeout。

### 示例

- connect timeout：Worker 网络问题
- first token timeout：排队或 Prefill 异常
- idle stream timeout：生成中卡住
- overall deadline：请求总体 SLA

具体值通过配置，不硬编码。

---

## 23. Retry（以响应提交为边界）

### 23.1 允许重试的完整条件

仅第一次 attempt、尚未尝试提交响应（commit_started=false）、客户端未断开、总 deadline 尚有预算、存在另一个可调度 Worker，且错误为连接失败/reset/提交前流异常截断/选定上游 502/503/504 时，跨 Worker 重试一次。

清理旧 upstream、释放旧 reservation、丢弃旧 attempt 的全部前导事件，再预占新 Worker。沿用同一个 request lease、原参数和总 deadline；首输出 deadline 从第一次转发起计算，不因 retry 延长。关闭 HTTP transport 自动重试。

业务 4xx/429、首输出/整体超时、客户端取消默认不重试。没有成功 attempt 时返回明确 HTTP 错误，不能返回空的成功 SSE。

### 23.2 已提交或提交结果不确定

一律禁止切换 Worker，保留已发送内容后关闭流，记录 partial_response_failure。不追加成功 [DONE]，不重放，不尝试更改 HTTP 状态码。秋招版不引入自定义 SSE error 协议；benchmark 把缺少合法终止的流视为失败。

这保证客户端不收到两个 attempt 拼接的输出，不承诺后端 exactly-once 执行。close 不能确认时隔离旧 Worker 并做真实取消验证。具体 S1–S6 测试见 `docs/核心正确性约定.md`。

---

## 24. Worker Health

Worker Health：

```text
HEALTHY
SUSPECT
UNHEALTHY
```

Worker Lifecycle：

```text
STARTING
ACTIVE
DRAINING
TERMINATED
```

两者分开。

### 24.1 Liveness

进程是否活着。

### 24.2 Readiness

是否应该继续接受请求。

例如：

- 模型还未加载完成
- KV Cache 初始化失败
- 发生 OOM
- 正在 drain

都可能：

```text
alive = true
ready = false
```

---

## 25. Heartbeat 与故障检测

### 25.1 Heartbeat

建议采样周期：

```text
1 second
```

连续超过阈值未更新：

```text
HEALTHY -> SUSPECT -> UNHEALTHY
```

阈值必须可配置。

### 25.2 多信号故障判断

不能只依赖 heartbeat。

还包括：

- HTTP connection error
- upstream timeout
- readiness probe
- metrics stale
- Kubernetes pod state

---

## 26. Worker Draining

扩缩容或滚动升级时：

```text
ACTIVE
  |
  v
DRAINING
  |
  | stop new requests
  |
  v
inflight == 0
  |
  v
TERMINATED
```

超过 drain deadline：

- 取消剩余请求
- 强制终止
- 记录 forced_drain_total

---

## 27. Scheduler 故障模型

v1 Scheduler 与 Gateway 同进程。

### 27.1 Scheduler State 分类

**可重建 soft state**

- Worker load
- Worker health
- Prefix affinity
- EWMA

**请求内 state**

- RoutingDecision
- 生命周期时间戳

v1 不保证 Gateway 进程 crash 后恢复正在 stream 的请求。

这是明确边界。

### 27.2 为什么可接受

本项目聚焦在线推理路由，不做 Durable Workflow。

用户请求本身是短生命周期 RPC/stream。

---

## 28. Rate Limiting

至少支持：

```text
per-tenant requests/min
per-tenant concurrent requests
global concurrent requests
```

v1 可以使用进程内 Token Bucket / Semaphore。

多 Gateway Replica 时再考虑：

```text
Redis-backed distributed rate limit
```

不提前引入。

---

## 29. 多租户 Prefix Cache 安全

多租户情况下，不应该无条件允许跨信任域 cache affinity。

策略：

```text
prefix_fingerprint =
hash(
    tenant_cache_salt
    + normalized_prefix
)
```

默认：

```text
不同 tenant 不共享 affinity
```

只有显式配置为同一 trust domain 时才共享。

---

## 30. Observability

必须同时覆盖：

1. Metrics
2. Logs
3. Traces

---

## 31. Metrics 设计

### 31.1 Gateway Metrics

```text
inferscale_requests_total
inferscale_request_errors_total
inferscale_request_duration_seconds
inferscale_active_requests
inferscale_active_streams
inferscale_request_cancelled_total
inferscale_request_timeout_total
```

Label 控制：

```text
model
status
routing_policy
```

禁止把：

```text
request_id
user_id
prompt
```

作为 Prometheus label，避免 cardinality 爆炸。

### 31.2 Scheduler Metrics

```text
inferscale_scheduler_decisions_total
inferscale_scheduler_latency_seconds
inferscale_worker_selected_total
inferscale_request_retries_total
inferscale_request_rejected_total
inferscale_worker_health
inferscale_worker_queue_score
inferscale_worker_cost_score
```

### 31.3 vLLM Metrics

重点采集：

```text
running requests
waiting requests
kv cache usage
prefix cache queries
prefix cache hits
prompt tokens
generation tokens
TTFT
ITL / TPOT
queue time
request success
preemptions
```

### 31.4 GPU Metrics

```text
GPU utilization
memory used
memory total
temperature
power
PCIe traffic
```

---

## 32. Logging

结构化 JSON：

```json
{
  "timestamp": "...",
  "level": "INFO",
  "event": "routing_decision",
  "request_id": "...",
  "model": "qwen3-8b",
  "policy": "prefix_aware",
  "worker_id": "worker-1",
  "queue_score": 0.31,
  "cost_score": 0.14,
  "prefix_affinity": 0.8
}
```

日志只保存：

- token count
- hash
- timing
- worker id
- error code

默认不记录原始 Prompt。

---

## 33. Distributed Tracing

OpenTelemetry Span：

```text
gateway.request
  |
  +-- validation
  +-- admission
  +-- scheduler.select
  +-- worker.connect
  +-- worker.first_token
  +-- stream.proxy
```

Trace 用于回答：

> TTFT 到底慢在 Gateway、Scheduler、队列还是模型 Prefill？

---

## 34. Grafana Dashboard

### 34.1 Serving Overview

- QPS
- Active Requests
- P50/P95/P99 E2E
- P50/P95/P99 TTFT
- TPOT / ITL
- Input tokens/s
- Output tokens/s
- Error Rate

### 34.2 Scheduler Dashboard

- 每 Worker 请求分布
- running/waiting requests
- Worker score
- routing latency
- retry rate
- rejection rate
- prefix affinity hit
- worker load variance

### 34.3 GPU Dashboard

- GPU utilization
- GPU memory
- KV Cache usage
- temperature
- power

---

## 35. Benchmark Framework（秋招版）

实现 `benchmark/workloads.py`、`runner.py`、`report.py`，保存 manifest、requests.jsonl、attempts.jsonl 和可重算汇总。

先用 closed-loop 固定并发冒烟，再用 open-loop 固定到达轨迹作主要策略比较。open-loop 预生成固定 seed 的 Poisson 计划，不等待请求完成再产生下一次到达；发送器没有空位时显式记录 not_sent，不能排队后仍声称维持原到达率。

记录 scheduled-to-sent lag；生成器受限/本地丢弃轮次标记无效，不作为正式性能收益。测量结束后有界 drain，窗口吞吐与包含 drain 的 cohort 完成情况分别输出。详细 B1–B6 用例见 `docs/核心正确性约定.md`。

---

## 36. Workload 定义

### W1：Short

```text
Prompt: 128-512
Output: 64-256
```

### W2：Long Prefill

```text
Prompt: 8K-16K
Output: 128-256
```

### W3：Long Decode

```text
Prompt: 256-512
Output: 1K-2K
```

### W4：Mixed

按固定随机种子混合：

```text
256
512
2K
8K
16K
```

### W5：Shared Prefix

```text
shared prefix = 4K / 8K
unique suffix = 32-256
```

这是 Prefix-aware 核心测试。

### W6：Hot Prefix + Imbalanced Load

制造：

```text
一个 prefix 大量重复
+
Worker A 初始高 load
```

用于验证 Cache Locality 与 Load Balance 的 trade-off。

---

## 37. Benchmark 控制变量（修订）

固定模型及 revision、tokenizer/chat template、采样默认值、precision、GPU 型号/数量、vLLM 版本和参数、请求数据、到达轨迹、seed、warmup、cache 开关和准备状态、测量时长。四种策略共用准入、超时、重试和容量限制。

主实验缩为 Mixed、Shared Prefix、Hot Prefix，对应 §36 W4/W5/W6。其它 workload 与完整 sweep 作为可选扩展。每个配置至少 3 轮、随机策略顺序；报告每轮结果及均值/范围，允许无收益或退化。

编译/模型预热和 prefix 预热分开；各轮统一重置或构建同样的缓存状态，重置 affinity/EWMA/账本。冷/热结果不能混在一起。参数在调参数据上选定，正式评估不再修改。

报告 requested output cap 和 observed output 分布；强制输出长度的合成实验明确标注。仅 fake Worker 数据不支持 GPU 性能结论。

---

## 38. Benchmark 指标与分母（修订）

所有客户端耗时来自同一 monotonic clock。每个计划请求保留一条样本，重试 attempt 单独记录。

| 指标 | 定义 |
|---|---|
| TTFT | 第一条非空内容到达 - 实际发送；role/usage 不算，无文本为 null |
| E2E | 终态 - 实际发送；成功延迟与失败耗时分列 |
| Chunk interval | 相邻 SSE 内容事件间隔，不是逐 token ITL |
| TPOT estimate | (末内容 - 首内容)/(usage output tokens - 1)，仅成功且 token 数可信且>1，标注 chunk 聚合误差 |
| Request throughput | 测量窗口内成功终结的逻辑请求数/窗口秒数，不能把 retry 算新成功 |
| Output throughput | 测量窗口内成功终结请求的 usage token 总数/窗口秒数，注明按完成请求归属 |
| Reject/error | 以 sent 为分母，同时报告占 planned 比例和 not_sent |
| Goodput | 窗口内成功且满足预先固定 TTFT/E2E SLO 的逻辑请求数/秒 |
| Cache hit | 后端 counter 窗口增量比，记录实际单位，独立于路由 affinity 命中 |

核心报告 TTFT/E2E P50/P95、成功吞吐、拒绝/错误率、各 Worker 请求分布。P99 在样本足够时补充；真实 queue/prefill/TPOT 使用锁定后端定义，没有数据就不报告。缺 usage 不以 chunk 数或 max_tokens 顶替。

停止到达并完成有界 drain 后，验证互斥分类：

```text
planned = not_sent + sent
sent = succeeded + rejected + failed + cancelled
```

timeout 是 failed 子类型，不重复计入。一个请求重试后成功计 1 个 success、2 个 attempt；EOF 无合法终止为失败。窗口后 drain 完成数只进 cohort，不塞进窗口吞吐分子。

正式汇总从原始样本生成，附样本量、全部结果分母和重复轮次差异；不能仅展示成功请求的低延迟。详细手算样本与检查见 `docs/核心正确性约定.md` §4。

---

## 39. 实验矩阵

至少完成：

### Experiment A：RR vs Least Load

Workload：

```text
Mixed
```

目标：

验证异构 request cost 下 RR 是否造成更大 queue imbalance。

### Experiment B：Least Load vs Cost Aware

Workload：

```text
Long Prefill + Long Decode mixed
```

目标：

判断 token cost 估计是否能够进一步降低 tail latency。

### Experiment C：Load Aware vs Prefix Aware

Workload：

```text
Shared Prefix
```

目标：

观察：

- Prefix cache hit
- TTFT
- Load variance

### Experiment D：Prefix Weight Sweep

```text
gamma =
0
0.25
0.5
1
2
4
```

目标：

找到 cache locality 和 load balance 的转折区域，而不是“证明 gamma 越大越好”。

### Experiment E：Overload

逐步提高 arrival rate。

观察：

```text
queue delay
TTFT
reject rate
throughput saturation
```

验证 Admission Control。

---

## 40. Fault Injection

### F1：Kill Worker

过程中 kill 一个 vLLM Worker。

记录：

```text
detection time
failed requests
retry success
recovery time
```

### F2：Slow Worker

人为增加网络延迟或限制 Worker。

观察 Scheduler 是否逐渐减少流量。

### F3：Metrics Stale

停止状态采集。

Scheduler 不允许永久使用过期 load。

超过：

```text
metrics_stale_threshold
```

降低 Worker 信任度或摘除。

### F4：Client Disconnect

大量客户端中途断开。

验证：

```text
upstream cancellation
resource release
```

### F5：Overload

请求速率超过服务能力。

验证：

```text
bounded queue
429/503
稳定而不是雪崩
```

### F6：Scheduler Restart

验证：

- Worker 状态重新发现
- readiness 恢复
- 新请求重新接入

不要求恢复旧 stream。

---

## 41. Configuration

建议：

```yaml
server:
  host: 0.0.0.0
  port: 8000

routing:
  policy: prefix_aware

  least_load:
    running_weight: 1.0
    waiting_weight: 1.5

  cost_aware:
    prompt_weight: 1.0
    output_weight: 1.0

  prefix_aware:
    queue_weight: 1.0
    cost_weight: 1.0
    kv_pressure_weight: 0.5
    affinity_weight: 1.0
    affinity_ttl_seconds: 120

admission:
  max_global_inflight: 256
  max_queue_depth: 256
  max_prompt_tokens: 16384
  max_output_tokens: 2048

timeouts:
  connect_seconds: 2
  first_token_seconds: 30
  idle_stream_seconds: 30

retry:
  max_before_first_token: 1
```

所有关键实验参数必须配置化。

---

## 42. 项目目录

```text
inferscale/
|
+-- app/
|   +-- main.py
|   |
|   +-- api/
|   |   +-- routes.py
|   |   +-- schemas.py
|   |   +-- auth.py
|   |
|   +-- gateway/
|   |   +-- proxy.py
|   |   +-- streaming.py
|   |   +-- admission.py
|   |   +-- cancellation.py
|   |
|   +-- scheduler/
|   |   +-- scheduler.py
|   |   +-- registry.py
|   |   +-- models.py
|   |   |
|   |   +-- policies/
|   |       +-- base.py
|   |       +-- round_robin.py
|   |       +-- least_load.py
|   |       +-- cost_aware.py
|   |       +-- prefix_aware.py
|   |
|   +-- workers/
|   |   +-- discovery.py
|   |   +-- health.py
|   |   +-- collector.py
|   |   +-- vllm_client.py
|   |
|   +-- telemetry/
|   |   +-- metrics.py
|   |   +-- logging.py
|   |   +-- tracing.py
|   |
|   +-- config/
|       +-- settings.py
|
+-- benchmark/
|   +-- runner.py
|   +-- workloads.py
|   +-- arrival.py
|   +-- analyzer.py
|   +-- report.py
|
+-- tests/
|   +-- unit/
|   +-- integration/
|   +-- load/
|   +-- chaos/
|
+-- deploy/
|   +-- docker/
|   +-- kubernetes/
|   +-- prometheus/
|   +-- grafana/
|
+-- scripts/
|
+-- configs/
|   +-- local.yaml
|   +-- benchmark.yaml
|
+-- docs/
|
+-- pyproject.toml
+-- Dockerfile
+-- docker-compose.yaml
+-- README.md
```

---

## 43. 测试策略

### 43.1 Unit Test

重点测试：

- 每种 routing policy
- score normalization
- stale Worker filtering
- affinity TTL
- admission limit
- retry eligibility
- state transition

要求 Scheduler 核心逻辑不依赖真实 GPU。

### 43.2 Integration Test

启动 fake vLLM worker：

可以模拟：

```text
queue
delay
first token delay
stream
500 error
disconnect
```

先验证系统语义。

### 43.3 GPU Integration

真实 vLLM + 小模型。

验证：

- OpenAI API
- streaming
- cancellation
- metrics
- routing

### 43.4 Load Test

至少：

```text
30-60 min sustained test
```

检查：

- memory leak
- connection leak
- metrics cardinality
- queue boundedness

---

## 44. 性能预算

InferScale 自身不能成为主要瓶颈。

目标：

### Scheduler

路由计算：

```text
P95 < 1 ms
```

在几十个 Replica 规模下。

### Gateway

除 Worker 等待之外额外代理开销：

```text
尽可能控制在低毫秒级
```

最终以 benchmark 数据为准。

注意：

文档中的性能预算是工程目标，不是已实现结果。

---

## 45. Kubernetes 设计

### 45.1 Gateway

Deployment：

```text
replicas >= 1
```

v1 单 Scheduler 情况下先 1 个实例。

### 45.2 vLLM Worker

每个 Pod：

```text
nvidia.com/gpu: 1
```

或 TP 场景：

```text
nvidia.com/gpu: 2
```

### 45.3 Probe

```text
startupProbe
livenessProbe
readinessProbe
```

三者分开。

### 45.4 Graceful Termination

收到 SIGTERM：

```text
mark DRAINING
stop accepting new requests
wait inflight
shutdown
```

配合：

```text
terminationGracePeriodSeconds
```

---

## 46. Autoscaling（v1.1 可选）

不把 HPA 的 CPU utilization 当核心指标。

候选扩容信号：

```text
waiting_requests
queue_delay
concurrency
TTFT degradation
```

例如：

```text
EWMA(queue_delay) > threshold
for sustained window
 -> scale out
```

缩容：

```text
low load for sustained window
 -> mark replica DRAINING
 -> scale in
```

v1 完整实现可选，但设计与指标必须预留。

---

## 47. 实现阶段（秋招版）

本节替代原先先高级策略、后可靠性、再 Kubernetes 的实现顺序。

| 阶段 | 交付 |
|---|---|
| P0 | 工程骨架、配置、领域类型、双 fake Worker、实验样本格式 |
| P1 | 唯一请求/成本账本、原子准入与预占、静态 Registry、RR |
| P2 | 两个文本 API、流式/非流式、提交边界、超时/取消/重试、基础 metrics/logs |
| P3 | 真实 vLLM 契约、tokenizer、Least Load/Cost/Prefix、两个 GPU Replica |
| P4 | 三类主实验、故障/过载演示、Compose、README、实验报告 |

P0–P2 与 P3 四策略 CPU 部分已实现，下一步上传服务器进行真实后端契约验证。详细文件、依赖和用例在 `docs/代码实现规划.md`，部署流程见 `deploy/README.md`；GPU 性能验收不能用模拟结果替代。

---

## 48. Definition of Done（秋招版）

- [ ] 一个 Gateway、两个独立真实 Replica；可单独启动 CPU fake 模式。
- [ ] 文本 chat/completion 流式与非流式、RR/Least Load/Cost/Prefix。
- [ ] 并发上限、健康门禁、超时、取消、提交前最多一次重试。
- [ ] 成本 C1–C5、账本 L1–L6、流式 S1–S6、实验 B1–B6 全部通过。
- [ ] 基础 metrics、结构化决策日志、一次 Worker failure 和 overload 演示。
- [ ] 三类主实验、可追溯配置和原始数据、至少三轮重复与真实报告。
- [ ] 核心单测/socket 集成、Docker Compose、README、架构图和实验说明。

本清单为当前交付标准。Kubernetes、多租户、HPA、完整 Grafana/DCGM/Tracing 不阻塞完成；未实现能力不写成简历成果。

---

## 49. 面试追问映射

### Q：vLLM 自己已经有 Scheduler，为什么还做？

回答核心：

vLLM Scheduler 主要管理单 Engine 内 sequence 执行；InferScale 做多个独立 Replica 之间的 request routing，两者层级不同。

### Q：为什么不用 Nginx Round Robin？

LLM request cost 高度异构，并且独立 Replica 拥有独立 KV Cache；普通 RR 不知道 queue、token cost 和 cache locality。

### Q：为什么不能看 GPU utilization 最低就发过去？

GPU utilization 是较粗且滞后的指标；LLM workload 还受到 queue、KV pressure、Prefill/Decode 和 cache locality 影响。

### Q：为什么 prompt token 可以表示成本？

它主要反映 Prefill 工作量的一部分，但不能单独表示总成本，因此还需要 output estimate 和 Worker 当前负载。

### Q：output token 不知道，怎么做 cost-aware？

用历史数据 EWMA 估计，并受 max_tokens 上界约束；预测误差本身也作为 benchmark 研究对象。

### Q：Prefix-aware 为什么可能变差？

Prefix locality 可能制造热点，使单 Worker queue 上升；因此不能做硬粘性，而要与 load score 联合。

### Q：应用层 Prefix fingerprint 和 vLLM KV block hash 一样吗？

不一样。v1 fingerprint 是 routing hint，不冒充 vLLM 的内部真实 block hash。

### Q：已经返回 token 后 Worker 挂了为什么不 retry？

因为客户端已经观察到部分输出；重新生成可能不一致或产生重复内容，所以默认终止 stream。

### Q：Scheduler 挂了怎么办？

Worker runtime 是 soft state，可以从 discovery、health 与 metrics 重建；v1 不恢复已经中断的 stream。

### Q：为什么不一开始用 Redis？

调度状态更新频繁、可重建且位于热路径，v1 内存化更简单且延迟更低；多 Gateway 时才引入分布式状态协调。

### Q：怎么证明策略有效？

固定模型、硬件、engine 参数、dataset、arrival process 和随机种子，只改变 routing policy，比较吞吐、TTFT、TPOT、tail latency、cache hit 与 load variance。

---

## 50. 项目最终简历表达目标

在拿到真实实验数据后，简历应该能够形成类似以下结构：

> 设计并实现多 Replica LLM Serving 平台 InferScale，基于 vLLM 构建 OpenAI-compatible 流式服务，在 Replica 层实现 Round-Robin、Load-Aware、Cost-Aware 与 Prefix-Aware 四类路由策略；结合 Worker queue、请求 token cost、KV Cache pressure 与 prefix locality 进行动态调度。

> （仅在完成对应扩展后使用）建立 Prometheus/Grafana/DCGM/OpenTelemetry 可观测体系，覆盖 TTFT、TPOT、queue time、KV Cache、GPU 与调度决策；实现 Admission Control、Backpressure、响应提交前的有界重试、client cancellation、健康检查与 graceful draining。

> 构建可重复 LLM Serving Benchmark，覆盖长短请求混合、Shared Prefix、Overload 与 Worker Failure workload，并通过控制变量实验量化不同路由策略在 tail latency、吞吐、cache hit 与负载均衡之间的 trade-off。

数字必须等项目真实运行后填写，禁止提前虚构。

---

## 51. 官方实现依据

设计中的关键事实应优先参考当前官方文档：

- vLLM OpenAI-Compatible Server
- vLLM Production Metrics / Metrics Design
- vLLM Automatic Prefix Caching
- vLLM Data Parallel Deployment
- vLLM Parallelism and Scaling
- NVIDIA DCGM Exporter
- Kubernetes Probe / Graceful Termination 相关文档

实现过程中应锁定实际使用的 vLLM 版本，并在仓库中记录版本号。后续升级版本时重新执行 Benchmark，避免框架升级造成实验不可比。

---

## 52. v1.0 最重要的原则

1. **不把 vLLM 的能力写成自己的实现。**
2. **任何优化必须有 baseline。**
3. **任何参数不能只靠拍脑袋，最终必须通过实验解释。**
4. **任何性能数字必须可复现。**
5. **路由策略不仅看平均延迟，更要看 P95/P99。**
6. **Prefix-aware 不默认等于更优，要研究 trade-off。**
7. **系统必须能在 overload 与 failure 下保持边界清晰。**
8. **优先把一个问题做深，而不是无限堆技术名词。**

InferScale v1.0 的最终价值不是“用了多少组件”，而是：

> 能用真实系统、真实 GPU 指标和可重复实验，解释多 Replica LLM Serving 中的调度与性能权衡。
