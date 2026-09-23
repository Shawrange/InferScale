# Hot Prefix：V1/V2 六轮实验

此文是服务器操作说明。本次交付只运行 CPU 验证，没有执行 GPU 性能实验。V1/V2 必须来自同一份新包；旧版 V1 结果不能直接并入。P3 取消沿用人工确认。

## 1. 更新和生成独立配置

将 `dist/inferscale-prefix-v2.zip` 解压到新目录，复制当前正在使用的 `configs/server.yaml`。保留原生双 vLLM 的启动参数、模型、tokenizer、endpoint、capacity 和 global_capacity；不覆盖旧目录、旧结果及 frozen traces。在新项目中激活 Python 3.12 环境，执行 `uv sync --locked`。后端继续使用原有环境。

下面的 `runs/p4-traces/hot_prefix.json` 指已经冻结的原文件：复制原文件到该位置，或把命令中的路径替换为其实际位置。不要重新 generate。

```bash
python -m inferscale.benchmark spec --config configs/server.yaml --output runs/v2-spec.json
python -m inferscale.benchmark plan --config configs/server.yaml --comparison-spec runs/v2-spec.json --output runs/v2-plan
cat runs/v2-plan/jobs.json
```

spec 从当前配置提取完整 routing，V1 显式 gamma=0.25，V2 显式 gamma=0.5、increment=0.25、retention=0.85、soft_limit=0.75。其他 routing 参数保持一致。`configs/prefix-v2-comparison.example.json` 是结构示例；实际运行优先用上面命令生成，避免覆盖服务器的非默认权重。

得到 `v1_g025.yaml`、`v2_g050.yaml` 及随机化六轮清单。每个 round 都包含两个 variant，各三轮；按 jobs.json 顺序执行。默认不带 spec 的 P4 plan 仍是四策略 36 轮。

## 2. 每轮准备

每轮按 P4 相同流程重启两个后端清空 prefix cache，使用完全一致的引擎预热；重启 Gateway 清空成功历史、EWMA 和账本。不能只为 V2 多做 warmup，不改变 frozen trace。`fresh_seeded` 是操作者的流程声明，工具不自动重启 GPU 后端。

首次部署可先用 `scripts/server_smoke.py --gateway ... --workers ... --model ... --expect-policy prefix_v2` 检查协议；随后重新执行上述缓存准备，再开始正式轮次。smoke 不是正式六轮之一。

## 3. 每轮两终端命令

下面演示 `v2_g050` 第 1 轮。按 jobs.json 替换 variant、policy 和 round；V1 对应 `v1_g025` / `prefix`，V2 对应 `v2_g050` / `prefix_v2`。

终端 A（旧 Gateway 已退出）：

```bash
mkdir -p runs/v2-logs
set -o pipefail
python -m inferscale.main --config runs/v2-plan/v2_g050.yaml --port 8000 2>&1 | tee runs/v2-logs/v2_g050-r1.log
```

终端 B（同一项目环境）：

```bash
python -m inferscale.benchmark run \
  --trace runs/p4-traces/hot_prefix.json \
  --config runs/v2-plan/v2_g050.yaml --policy prefix_v2 \
  --comparison-spec runs/v2-spec.json --variant-id v2_g050 --round 1 \
  --gateway http://127.0.0.1:8000 --mode open_loop \
  --capacity 32 --timeout 60 --drain 65 --lag-tolerance 0.05 \
  --ttft-slo 1 --e2e-slo 10 --cache-condition fresh_seeded \
  --environment runs/p4-environment.json \
  --gateway-log runs/v2-logs/v2_g050-r1.log \
  --output runs/v2-results/v2_g050-r1
```

上面的 client capacity/超时/SLO 是工具原默认值；如果上一轮冻结的实验采用其他值，两组统一沿用实际值，不为单个变体调整。`runs/p4-environment.json` 继续使用核对后的真实记录，不保留模板占位符。已有 output 目录不会覆盖。

manifest 保存完整配置、源码摘要、variant_id、spec 摘要、每个 Worker 的 q 上界及 soft limit 是否可达。run 会核对 variant 的每个 routing 字段及 workload，策略与 Gateway 不一致时不发送测量请求。

两组完整结果为：v1_g025-r1/r2/r3 和 v2_g050-r1/r2/r3。一轮结束且日志落盘后停掉 Gateway，按相同准备流程进入下一轮。

## 4. 汇总与回传

```bash
python -m inferscale.benchmark compare runs/v2-results/* --comparison-spec runs/v2-spec.json --output runs/v2-comparison
tar -czf inferscale-prefix-v2-results.tar.gz runs/v2-results runs/v2-comparison runs/v2-plan runs/v2-spec.json runs/p4-traces/hot_prefix.json runs/p4-environment.json
```

日志尚未完全落盘时可先复算该轮：

```bash
python -m inferscale.benchmark report runs/v2-results/v2_g050-r1 --gateway-log runs/v2-logs/v2_g050-r1.log
```

默认 compare 不带 spec 时仍严格分组，V1/V2 的不同 gamma 不会被悄悄忽略。显式比较仅允许规格声明的路由差异；模型、容量、源码、轨迹、环境、缓存准备、负载和 SLO 等仍必须相同。不同条件会分组，不能跨组宣称收益。每个 variant 至少三轮有效结果，不足时报告会标明。

检查各轮 `summary.json` 的完整分母、发送器受限及排除原因；`routing_decisions` 分别以已关联的全部/首次/重试 attempt 为分母，缺失与真实零分开计数。`routing_sequence` 保留日志决策先后顺序。V1 没有新增分解快照，相关字段为缺失，不当作 gate=0；V1/V2 均有 affinity。

比较 TTFT/E2E P95、窗口吞吐、拒绝率、Worker 分布，以及 V2 的 q/gate/bonus、亲和饱和比例。缓存差分仍是 cohort 观察，不当作请求级缓存命中。若 q 很低或亲和很快双侧饱和，可报告未触发机制或没有收益，不改冻结负载追求正结果；暂不重跑全部 P4。
