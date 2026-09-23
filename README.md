# InferScale

秋招项目：在独立 vLLM Replica 之上比较请求数、未完成成本和前缀亲和路由。

**P0–P3 已按秋招项目范围验收通过：可靠生成代理、四种路由、成本预测、Prefix 亲和，以及真实 tokenizer 接口。双 4090 服务器的四策略协议/usage 和路由日志已核对，GPU 取消由用户人工确认。P4 实验工具已实现并通过 CPU 验证，下一步上服务器试跑并收集正式结果。** 见 [P3验收结论](docs/validation/P3验收结论.md)。

当前 P4 代码：157 项测试通过。P2 历史版本另有 20 分钟真实 socket 验收：7292 次成功、3646 次后端已确认取消，最终账本归零，见 [P2 验收](docs/validation/P2验收.md)；不将其冒充 P3 版本的长跑结果。

## 已实现

- Python 3.12 工程、`uv.lock`、严格 YAML 配置、基础 CPU CI。
- request/attempt 分离、原子准入和 RR 预占、每 Worker 容量、固定成本 reservation。
- 幂等释放、取消/异常统一清理、request ID 复用与旧实例释放保护。
- 静态 Worker、健康探测、TTL、drain/SUSPECT 门禁；不让过期探测恢复旧实例。
- 两种文本 API 的 fake Worker：成功、首内容前/后截断、502/503/504、空完成、延迟。
- 实验 request/attempt 样本类型，未知 token usage 保持 null。
- 有界 SSE 预取、显式响应提交边界，提交前最多一次跨 Worker 重试。
- 请求体/响应体限额、超时、客户端断开、上游关闭与清理失败隔离。
- 基础 metrics、JSON 决策/终态日志；真实 TCP 慢读、取消及故障注入测试。

Cost-Aware 评分、EWMA 和 Prefix 已实现，配置 `routing.policy` 可选 `round_robin/least_load/cost/prefix`。fake usage 是确定性测试单位，不能作为真实 token/GPU 性能数据。接口语义见 [API 范围](docs/API范围.md)，本轮进度见 [P3 实现与验收](docs/P3实现与验收.md)。

上传租用服务器：见 [部署说明](deploy/README.md)。运行 `python scripts/package_release.py` 可生成 `dist/inferscale-p4-experiments.zip` 及 SHA-256 校验文件；包内有锁定依赖、CPU/GPU Compose、配置生成器、协议验收脚本，不含模型、密钥或虚拟环境。

P4 新增固定轨迹 open-loop/closed-loop runner、请求与重试日志关联、完整终态分母、窗口吞吐及重复轮次比较。见 [P4 实现与验收](docs/P4实现与验收.md) 和 [服务器操作步骤](deploy/P4实验运行.md)。先跑一次冒烟和一个 open-loop 轮次，再确定正式负载；尚无真实性能收益数字。

## 安装与验证

要求 Python 3.12。项目虚拟环境不修改全局依赖。若已有 uv，直接执行 `uv sync --locked`；没有 uv 时可用已有 pip 引导：

```powershell
python -m venv .venv --without-pip
python -m pip --python .venv install uv==0.12.17
.\.venv\Scripts\uv.exe sync --locked
.\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider
.\.venv\Scripts\ruff.exe check src tests scripts
.\.venv\Scripts\ruff.exe format --check src tests scripts
```

Linux/macOS 有 uv 时：

```bash
uv sync --locked --python 3.12
uv run --no-sync pytest -q
uv run --no-sync ruff check src tests scripts
```

`-p no:cacheprovider` 仅避免当前 Windows 沙箱对 pytest 缓存目录的权限警告，不影响测试执行。集成测试监听随机本地端口，自动退出并清理，不要求手动启动 Worker，也不需要 GPU。CI 文件已提供，但未声称 GitHub 上已运行通过。

## 启动两个模拟 Worker

在项目根目录分别开启三个终端。所有服务默认只绑定 loopback。

终端 1：

```powershell
.\.venv\Scripts\python.exe -m inferscale.testing.worker --worker-id worker-0 --port 8100
```

终端 2：

```powershell
.\.venv\Scripts\python.exe -m inferscale.testing.worker --worker-id worker-1 --port 8101
```

终端 3（Gateway）：

```powershell
.\.venv\Scripts\python.exe -m inferscale.main --config configs/local.yaml --port 8000
```

检查：

```powershell
Invoke-RestMethod http://127.0.0.1:8000/health/ready
Invoke-RestMethod http://127.0.0.1:8100/_test/requests
```

两个 Worker 健康时 readiness 返回 `ready_workers: 2`；全部不可用时返回 503。`/_test/requests` 仅存在于 fake Worker，记录最近 256 次请求的 ID/结果，不记录 prompt。

通过 Gateway 调用：

```powershell
$body = @{ model = 'fake-model'; messages = @(@{role='user'; content='hello'}); stream = $false } | ConvertTo-Json -Depth 4
Invoke-RestMethod http://127.0.0.1:8000/v1/chat/completions -Method Post -ContentType 'application/json' -Body $body
```

观察流式响应（另一个文本接口）：

```powershell
'{"model":"fake-model","prompt":"hello","stream":true}' | curl.exe -N http://127.0.0.1:8000/v1/completions -H 'Content-Type: application/json' --data-binary '@-'
```

成功流包含 finish 事件和 `[DONE]`；中途失败可能已是 HTTP 200，但缺少合法结束，客户端必须识别为失败。向 `configs/local.yaml` 添加 `api_key_env: INFERSCALE_API_KEY` 并设置同名环境变量可启用 Bearer 鉴权。

停止对应 Worker 后修改启动参数即可切换场景：

| 参数 | 行为 |
|---|---|
| `--scenario success` | role、内容、finish、`[DONE]` |
| `--scenario before-content` | 只有 role 后 EOF，没有合法结束，供预提交重试测试使用 |
| `--scenario after-content` | 首内容后 EOF，无 `[DONE]`，供提交后失败测试使用 |
| `--scenario http-error --error-status 503` | HTTP 503；也支持 502、504 |
| `--scenario empty` | 合法空完成，不能记文本 TTFT |
| `--first-delay 5 --chunk-delay 0.2` | 首内容和各块延迟，供断开与超时测试使用 |

截断场景模拟的是“不完整 SSE + HTTP EOF”，不是 TCP reset。它们只适用于流式请求；不将这些 fault 参数混入网关的公共请求协议。

## 账本演示

```powershell
.\.venv\Scripts\python.exe scripts/demo_ledger.py
```

纯内存演示 A 的 9000 成本释放、B 预占 1000，整个过程逻辑请求数为 1，结束后所有活跃数和成本归零。它不发推理请求，不证明网络重试安全；两个真实本地 socket 的 RR 调度测试见 `tests/integration/test_fake_workers.py`。

单进程状态只支持一个 Uvicorn worker。Worker epoch 变化需显式确认实例重启，不能由一次 HTTP 200 猜测；已接入上游句柄的 cleanup，P3 再验证真实 vLLM 的取消和指标。

CPU 稳定性验证（约 20 分钟，结果不代表 GPU 性能）：

```powershell
.\.venv\Scripts\python.exe -m tests.soak --seconds 1200 --output runs/p2-soak.json
```

## 下一步

P3 剩余：在租用服务器上对两个独立 vLLM Replica 验证模型/template、协议、usage、取消和缓存指标。P4 再做三类实验及可复算报告。

完整计划见 [代码实现规划](docs/代码实现规划.md)，正确性约定见 [核心正确性约定](docs/核心正确性约定.md)，本轮验证及边界见 [P2 验收](docs/validation/P2验收.md)。
