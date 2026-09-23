# P2 文本生成接口与失败语义

当前支持 CPU fake Worker 和 vLLM tokenize/生成适配接口，四种路由可配置。以下是已实现范围，不等同于完整 OpenAI API 或真实 GPU 验收。

## 请求

`POST /v1/chat/completions`：`model`、`messages` 必填。消息只允许 `system/user/assistant`，content 为纯文本。

`POST /v1/completions`：`model`、单个字符串 `prompt` 必填。

两者支持 `stream`（默认 false）、`max_tokens`（缺省用模型配置）、`n=1`、`temperature`、`top_p` 和 `stream_options.include_usage`。不支持 tools、图像、多个 choice、批量 prompt、请求级模板/缓存盐等扩展；未知字段返回 400，不静默丢弃。

所有请求走同一个 global lease 和 Worker 容量门槛，网关不另设等待队列。API Key 可通过配置 `api_key_env: INFERSCALE_API_KEY` 引用环境变量，客户端发送 `Authorization: Bearer ...`；未配置时是 loopback 自用模式。密钥不会转发到后端。

默认 tokenizer 为显式 `fixture-v1`：chat 使用 2 + 每条消息 (4 + UTF-8 字节数)，completion 使用 1 + UTF-8 字节数。它只用于与 fake 后端一致的边界测试，**不是任何真实模型的 token 数**。真实模型使用 `tokenizer_mode: vllm`，请求健康后端 `/tokenize` 获取模板 token IDs，并在服务器上验证与生成 usage 一致；应用拒绝把默认 fixture 直接用到真实模型。

tokenize 独立并发槽满时立即 503；远端失败或返回不一致的 count/IDs 时明确报错，不以字符数兜底。该阶段计入入站 overall deadline，首输出的上游等待仍从首次生成 forward 开始。

## 响应

保留上游 response ID、文本、choice、finish reason 和 usage。每次用户请求生成一个 `X-InferScale-Request-ID`，内部 retry 沿用该逻辑 ID、使用新的 attempt ID。用户凭据和任意请求 Header 不透传。

流式输出按 SSE 事件增量转发；只统一 CR/CRLF/LF 行结束格式，不拼接两个 attempt 的内容。收到首有效文本或合法空完成前缓存少量前导事件，不提前发 HTTP 200。

首文本内容前的 role、空 delta、usage、heartbeat 不记 TTFT。空完成和非流式响应不生成虚假的 TTFT。`/metrics` 中的 first-output wait 是网关观察的上游等待，客户端 TTFT 仍需基准客户端单独测量。

## 错误和重试

| 条件 | 行为 |
|---|---|
| 未鉴权 | 401，不创建 Worker attempt |
| 未知参数/模型、上下文/输出超限 | 400 |
| 实际读取的请求体超限（含 chunked） | 413 |
| 全局容量不足或无可调度 Worker | 503，快速拒绝 |
| 上游 400/429 | 对应错误状态，不重试 |
| 上游 500 或协议/体积错误 | 502，不重试 |
| 提交前连接失败/reset/流截断/上游 502/503/504 | 有另一个可用 Worker 时最多重试一次 |
| 首输出/整体/读写等待超时 | 提交前 504；提交后中止，不重试 |
| 客户端断开 | 取消正在读取的上游，关闭句柄，回收账本 |
| 响应头已尝试发送（包括 send 抛错） | 禁止重试，不能更改已提交状态码 |

流中故障以连接结束且**缺少合法 `[DONE]`** 表示，不追加伪成功结束，不引入自定义 SSE error 协议。客户端不能仅凭 HTTP 200 或 EOF 判定生成成功。非流式响应有大小上限，完整、结构有效后才提交。

HTTP transport 隐式重试关闭。首输出和总体 deadline 跨 retry 不重置；同一 request lease 始终只占一个逻辑请求配额。旧 attempt 的 role/ID/前导事件在重试前丢弃。

## 有界资源

| 配置 | 默认值 |
|---|---:|
| max_body_bytes | 1 MiB |
| max_response_bytes（非流式） | 4 MiB |
| max_event_bytes | 64 KiB |
| max_prefetch_bytes | 128 KiB |
| global_capacity | 8 |
| 每 Worker capacity（local.yaml） | 4 |
| body / connect / pool timeout | 5 / 2 / 1 秒 |
| first-output / idle / overall timeout | 30 / 30 / 60 秒 |
| downstream send / cleanup timeout | 5 / 1 秒 |

连接池有界；下游读得慢时不无限预读，达到 send timeout 后清理。关闭失败/超时或者请求发出但尚未取得上游 response 就被中止时，旧 Worker 保守进入 SUSPECT；仅 HTTP 健康探测不能将其恢复。当前需显式确认实例重启后恢复，真实后端负载核验与自动恢复留待 P3。

关闭连接并不证明真实 GPU 推理已经取消；当前通过的是 fake Worker 和 socket 验收，vLLM 取消行为必须在 GPU 阶段另验。

## 核心验收对应

- S1/S2/S4/S5/S6：`tests/integration/test_gateway.py`，真实 socket 的切换、截断、空完成、超时及断开。
- S3：`tests/unit/test_proxy_lifecycle.py` 的 ASGI send 故障注入，确定性验证“响应头提交结果不明也不重试”。
- L2/L5：首输出等待/输出中/连接中取消和 cleanup 抛错/超时；检查本地账本归零、旧 Worker 隔离。
- 慢客户端：`tests/integration/test_gateway_backpressure.py` 停止读取实际 TCP 响应，验证 backpressure timeout。
- SSE 边界：`tests/unit/test_sse.py` 逐字节 UTF-8、跨包、CR/CRLF、多事件、非法事件及限额。
- 稳定性：`python -m tests.soak --seconds 1200` 混合成功/主动取消，采样任务数、Python 分配内存、上游连接和账本；最终结果以生成的报告为准。
