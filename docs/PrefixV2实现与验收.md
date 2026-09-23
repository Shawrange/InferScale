# Prefix-Aware V2 实现与 CPU 验收

日期：2026-09-23。V2-0 至 V2-4 完成；没有执行 GPU 性能实验、提交或推送。

## 基线

远端 HEAD：`8ee9878b45e83726c79c2f1bc572f7cd711af939`。先通过只读 ls-remote 查询，再克隆到工作空间的独立 runs 目录核对；本地改动前 src/tests/scripts/configs 文本与该 HEAD 一致，未发现 AGENTS.md。

工作空间根目录没有 .git，改动前保存了 137 个文件及 SHA-256 摘要。基线全量测试：157 passed，70.32 秒。没有初始化或覆盖工作空间 Git 历史。

## 实现

V1 的二值索引、默认 gamma=0.25、打分、容量过滤、重试与成功学习行为保留。V2 使用独立 SuccessPrefixIndex，以成功写入刷新 TTL 和淘汰顺序，读取不刷新。配置 gamma=0.5、increment=0.25、retention=0.85、soft_limit=0.75。

```text
affinity_new = min(1, affinity_old * retention + increment)
q = 当前 Worker、当前 epoch 的活动 reservation 数 / capacity（本次预占之前）
base = queue_weight*q + cost_weight*outstanding/cost_reference
gate = max(0, 1-q/soft_limit)
effective_gamma = gamma*gate
rank = base-effective_gamma*affinity
score = rank+cost_weight*current_request_cost/cost_reference
```

过期/不存在时 old=0，成功记入 expires_at=now+TTL。这不是时间衰减，默认连续成功第六次饱和。按 rank 选择，同分 RR；成本不可信时退回 RR，分解快照为空。快照在原锁内产生，routing_decision 和 attempt_finished 复用同一不可变对象。

CLI 新增 spec 生成、run 的 comparison-spec/variant-id、compare 显式规格及 plan 可选策略集。spec 要求完整 routing 配置，只允许预定五个处理字段不同。默认 compare 保持严格分组；显式规格也不会忽略容量、模型、源码、环境或 trace 差异。默认 plan 仍四策略 36 轮，显式 Hot Prefix 规格生成六轮。

## 新增验证

新增与扩展共 44 个测试实例：

- 成功分数递推、饱和、空闲不变、TTL 等号到期/重建、读取不刷新淘汰、独立 Worker、None 前缀、代际失效。
- 三个参数的零/负/超界/NaN/Infinity 拒绝；gate 四个负载边界、不可变快照、公共成本项、低负载亲和、高负载 Cost 排序、高成本压过亲和、gamma=0 等价和 fallback。
- V2 完整 stream/non-stream、失败/超时/取消不学习、失败 A→成功 B 只学习 B 一次；V1/V2 成功但 usage 缺失仍记亲和。
- 日志快照一致、篡改拒绝、attempt 分母、缺失不当零、从 CLI 原始样本到报告/显式比较的 socket 链路。
- 默认 36 轮、显式六轮、不改非 routing 配置、已有目录不覆盖；真实 socket /tokenize 与 12 项协议 smoke。
- 默认严格分组、显式变体同组、容量/trace/模型/源码变化保持隔离、未声明 gamma 拒绝、不完整 routing 拒绝、同 policy 多变体与重复轮次判定。

最终全量 pytest：**201 passed，86.13 秒**。Ruff check 和 format --check 通过，52 个 Python 文件格式一致。离线 uv lock --check 通过，依赖未变。既有 workloads 和原配置文件保持原字节不变。

部署包独立验证：109 个文件 SHA-256 核对通过；强制从解压包内 src 导入，44 项 V2 算法、比较规格、plan 和真实 socket/CLI 冒烟全部通过（16.37 秒）。

## 兼容与交付边界

新决策字段为可选，旧 request/attempt 样本无需新增必填字段，旧 schema_version 不改。旧报告仍可读取，但旧代码产生的正式结果不与新版结果自动合并。V2 新日志缺失或篡改分解快照会被报告拒绝。

V1 分解字段为空，不能与 V2 的真实零混淆；分布以已关联 attempt 为分母，首次与重试分别汇总，缺失单列。亲和是历史启发式，不等同真实缓存命中。没有证明 V2 性能更优。

部署包为 `dist/inferscale-prefix-v2.zip`；旧 P4 包保持保留。下一步按 [Hot Prefix 六轮说明](../deploy/PrefixV2六轮实验.md)，在同版代码上运行 V1/V2 各三轮。只生成运行说明，不自动操作 GPU 后端。

完整修改文件及 Git diff 统计见 [变更统计](validation/PrefixV2变更统计.md)。
