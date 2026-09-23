# 上传服务器后运行

此包包含 P3 CPU 策略实现和 GPU 启动候选配置；不含模型权重、Python 虚拟环境或缓存。先跑 CPU，再跑 GPU。四策略的性能报告与 open-loop 实验尚未实现，`server_smoke.py` 的耗时不能当性能比较。

## 1. 解压与安装

目标环境：Linux、Python 3.12。Docker 路径需要 Docker Engine、Compose v2；GPU 容器还需 NVIDIA 驱动和 Container Toolkit。当前 GPU Compose 明确使用 GPU 0 和 1，各一个独立 TP=1 引擎；两卡各自必须能容纳同一模型及 KV cache。不要先按未知模型盲租机器，先确定模型、精度、上下文和单副本显存需求。

```bash
sha256sum -c inferscale-p3-server.zip.sha256
unzip inferscale-p3-server.zip
cd inferscale
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install uv==0.12.17
uv sync --locked
python -m pytest -q -p no:cacheprovider
```

归档内 `PACKAGE-MANIFEST.json` 为每个源文件的 SHA-256。模型自行放在服务器的不可变 snapshot 目录中，包含 tokenizer、chat template、配置和权重。

## 2. CPU 冒烟（无 GPU）

```bash
docker compose -f deploy/compose.cpu.yaml up -d --build
curl -f http://127.0.0.1:8000/health/ready
python scripts/server_smoke.py --model fake-model --output runs/cpu-smoke.json
docker compose -f deploy/compose.cpu.yaml down
```

readiness 启动初期可能是 503，等两个 worker 健康后再运行 smoke。不能运行 Docker 时，按根目录 README 在三个终端启动 fake Worker 和 Gateway，再执行同一 smoke 命令。

## 3. GPU 启动

```bash
cp deploy/gpu.env.example deploy/gpu.env
# 编辑 deploy/gpu.env：MODEL_DIR 填服务器模型绝对路径；核对 GPU_0/GPU_1。
python scripts/render_server_config.py --model-identity '你的模型仓库@固定commit及模板版本' --policy round_robin --context-limit 2048
docker compose --env-file deploy/gpu.env -f deploy/compose.gpu.yaml config --quiet
docker compose --env-file deploy/gpu.env -f deploy/compose.gpu.yaml up -d --build
docker compose --env-file deploy/gpu.env -f deploy/compose.gpu.yaml logs --tail 100
curl -f http://127.0.0.1:8000/health/ready
python scripts/server_smoke.py --output runs/gpu-smoke.json
```

`CONTEXT_LIMIT` 必须与 render 命令一致。服务名固定为 `inference-model`；两个后端挂载同一只读模型目录。选择非 reasoning 的纯文本 instruct 模型作为首个契约验证对象；本接口暂不转译 reasoning/tool 输出。若模型不能容纳，不靠扩大 Gateway 限额解决，先调整模型/上下文与后端资源。

示例固定使用 `vllm/vllm-openai:v0.23.0` 作为**待硬件验证候选**，不是已验证组合。可改为你平台兼容的明确 tag/digest；不要用 latest。本机未运行 GPU 容器。正式结果需额外保存实际镜像 digest、驱动/GPU、模型 commit 与完整启动参数。

Gateway 的 `tokenizer_mode: vllm` 调用健康后端的 `/tokenize`，获得真实模板 token IDs；独立连接池、并发槽、响应大小与 timeout 有界，不会用字节数兜底。这会增加一次渲染网络往返，四策略实验必须保持同一设置。服务启动后的 smoke 对比两个后端的 chat/completion token IDs，并核对 direct/Gateway、stream/non-stream 的 usage 和终止协议。若任一项失败，先保留日志修复，不进入正式收益实验。

默认端口只映射到服务器 loopback，可用 `ssh -L 8000:127.0.0.1:8000 用户@服务器` 在本地访问。单进程 Gateway，不增加 Uvicorn worker 数。

## 4. 切换策略与留存结果

`--policy` 可用 `round_robin`、`least_load`、`cost`、`prefix`。重新生成 server.yaml 后重启 gateway，让 EWMA/affinity/账本从干净状态开始；不在正式轮次中途切策略。

```bash
python scripts/render_server_config.py --model-identity '同一个不可变identity' --policy cost --context-limit 2048
docker compose --env-file deploy/gpu.env -f deploy/compose.gpu.yaml restart gateway
python scripts/server_smoke.py --output runs/gpu-cost-smoke.json
docker compose --env-file deploy/gpu.env -f deploy/compose.gpu.yaml logs --no-color > runs/gpu-services.log
docker compose --env-file deploy/gpu.env -f deploy/compose.gpu.yaml images > runs/images.txt
nvidia-smi > runs/nvidia-smi.txt
```

下一次把 `runs/gpu-smoke.json`、服务日志、镜像/GPU 信息和脱敏配置带回即可继续适配。关闭服务：`docker compose --env-file deploy/gpu.env -f deploy/compose.gpu.yaml down`。

## 5. 必须单独完成的 GPU 验收

协议 smoke 不证明取消成功。还需独占两副本，在可持续生成的请求中途关闭客户端，记录请求 ID、Gateway 终态和 vLLM 的 running/waiting 指标及日志，确认请求停止；不能仅凭稍后 running=0 排除“自然生成完毕”。Prefix affinity 也只是路由启发式，不等于真实 cache hit。缓存指标、三类 workload 和至少三轮固定轨迹对比在 P4 完成。

核对依据（2026-09-22）：[vLLM Docker 文档](https://docs.vllm.ai/en/latest/deployment/docker/)、[v0.23.0 serve 参数](https://docs.vllm.ai/en/v0.23.0/cli/serve/)、[v0.23.0 tokenize 协议源码](https://github.com/vllm-project/vllm/blob/v0.23.0/vllm/entrypoints/serve/tokenize/protocol.py)。
