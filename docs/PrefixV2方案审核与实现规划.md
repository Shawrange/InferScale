# Prefix-Aware V2 方案审核与实现规划

日期：2026-09-23。状态：V2-0 至 V2-4 已完成代码及 CPU 验证；V2-5 GPU 实验未执行。详见 [实现与验收](PrefixV2实现与验收.md)。

审核输入为 `InferScale_Prefix_Aware_V2_Codex_Plan.md` 全文及当前工作空间源码。目标仓库为 https://github.com/Shawrange/InferScale ，设计审核时远程读取失败，审核基于本地 P4 实现；实现阶段已核对远端 HEAD 8ee9878b45e83726c79c2f1bc572f7cd711af939，核心源码与本地基线一致。附件中的服务器路径和执行指令属于方案内容，审核阶段仅更新规划；此后依用户明确授权实施 V2-0 至 V2-4，不执行部署及 GPU 性能实验。未发现适用 AGENTS.md。

## 1. 审核结论

建议修订后实施为独立的 `prefix_v2` 实验策略。保留 V1、容量门槛、原子预占、取消/重试和固定轨迹，工程改动可控制在现有模块内。V2 是成功历史驱动的路由启发式，不是后端缓存命中率估计，也不能预先保证延迟改善。

附件称 P4 中 V1 没有收益、增大 gamma 后分流偏斜。这里将其作为用户提供的实验背景；本轮未复算完整正式实验及 gamma 扫描。分布偏斜且延迟不变，不能单独证明固定 gamma 是根因，也可能是前缀过短、两副本都已缓存、负载太低或解码占主导。无需为启动 V2 设计再追补 P3 取消证据，继续采用用户人工确认。

## 2. 必须修订的点

| 优先级 | 发现及依据 | 实现决定 |
|---|---|---|
| 高 | 原方案 §4 的 `old * 0.85 + 0.25` 仅在成功时执行；读取返回原分数，`updated_at` 未参与计算。它不是随时间衰减。 | 首版保留简单递推，称为“成功历史亲和分数 + TTL”，类名建议 `SuccessPrefixIndex`，配置建议 `prefix_history_retention`。不宣称时间衰减，不额外引入半衰期。 |
| 高 | `benchmark/compare.py` 只从分组配置中移除 policy，仍比较 gamma 和源码摘要。V1 gamma=0.25 与 V2 gamma=0.5 会被分组；旧版 V1 与新版 V2 也不在同组。 | 增加显式实验对比规格，限定允许变化的路由字段；默认严格比较保持原样。在同一新版代码上重新运行 V1/V2，旧结果仅作为历史背景。 |
| 中 | 默认递推第六次成功就饱和为 1，不能长期保证“成功更多的 Worker 分数更高”。A/B 两边饱和后差异消失。 | 记录并测试饱和；比较“4 次 vs 1 次”可成立，但不得推广成无限单调区分能力。报告饱和比例及时间顺序。 |
| 中 | `q` 在本地源码中是预占前该 Worker 的活动 attempt 数 / capacity，不是 Gateway 全局请求数，也不是 GPU 利用率。全局容量可能使 0.75 阈值不可达。 | 明确定义，预检记录容量和实际 q/gate 分布；不为触发降权偷偷提高容量。 |
| 中 | 同时改 gamma、亲和更新和 load gate，V1/V2 两组只能评价整个策略组合。 | 主实验允许比较组合，但不把收益单独归因于 gate 或连续分数；需要机制归因时再补小规模消融。 |
| 中 | 原方案 H 将“高负载关闭 bonus”等同于“必选更空 Worker”。关闭 bonus 后仍有 outstanding cost 项。 | 测试显式控制成本，使更空 Worker 的 base score 也更低；验证高负载退回 Cost 排序，不承诺总选请求数最少者。 |
| 中 | 当前 proxy 在请求完成后记亲和，不以 `predictor.observe()` 返回 true 为条件；原方案“无可信结果”没有定义。 | 保持 V1 语义；V2 同样要求完整协议成功和 epoch/generation 匹配。usage 缺失只阻止成本预测学习，不单独否决成功历史。失败、超时、取消不更新。 |
| 中 | `POLICIES` 同时用于 run choices 和 plan；直接扩容会把默认 36 轮变成 45 轮。`server_smoke.py` 也有两处固定枚举。 | 拆分“支持策略”和“默认实验策略”；默认仍四策略 36 轮，V2 用显式选择生成。同步 smoke、配置和报告解析。 |

## 3. 冻结的算法约定

对于已通过健康、容量、重试候选过滤的同构 Worker i，在同一把 registry/ledger 短锁内读取状态并完成选择与预占：

```text
n_i = 该 Worker 当前 epoch 的活动 reservation 数（不含本次预占）
q_i = n_i / capacity_i
base_i = queue_weight * q_i + cost_weight * outstanding_i / cost_reference
gate_i = max(0, 1 - q_i / prefix_load_soft_limit)
effective_gamma_i = gamma * gate_i
bonus_i = effective_gamma_i * affinity_i
rank_i = base_i - bonus_i
score_i = rank_i + cost_weight * current_request_cost / cost_reference
```

按 rank 最小选择，同分继续使用现有 RR。当前请求成本只加到返回/log score，同构池内不参与候选比较。所有 q 都取决策时快照，不能在预占后或流结束时重新计算日志字段。成本状态不可信时退回 RR，并记录 fallback_reason；此时 V2 分解字段为空，不能伪装成 V2 决策。

成功历史更新：

```text
old = 0，若无条目、条目过期，或 Worker 身份代际已变化
new = min(1, old * prefix_history_retention + prefix_hit_increment)
expires_at = now + prefix_ttl_seconds
```

默认 retention=0.85、increment=0.25、soft_limit=0.75；V2 配置显式 gamma=0.5。全局 RoutingConfig 的 gamma 默认继续为 0.25，旧 prefix 配置不变。三个新参数均有限且在 (0,1]。

使用 monotonic clock。读取不改变 score、顺序或 TTL；成功写入刷新 TTL 并移至 OrderedDict 末尾。准确称为“按最近成功写入淘汰”，不声称每次读取都刷新 LRU。key 沿用 worker_id、epoch、affinity_generation、prefix；prune 处理 TTL 和健康代际，最大条目数沿用现有上限。保留 fingerprint 的模型/tokenizer/template 身份与固定前 N token 语义。

默认连续更新为 0.25、0.4625、0.643125、0.79665625、0.92715781、1。空闲但 TTL 未到时分数不变。固定的一次 seed warmup 后，V2 在 q=0 时 bonus=0.5×0.25=0.125，小于 V1 的 0.25；不能为 V2 单独增加 warmup。若后续确实需要随时间衰减，应另立变更，用明确时间单位和半衰期，避免悄然改变本轮算法。

阈值可达性：新请求已通过全局准入时，通常有 `n_i <= min(capacity_i-1, global_capacity-1)`。例如 C=16、G=8，q 最大 7/16，无法达到 0.75，但 gate 仍会部分下降。此例只是说明边界，不是当前服务器配置结论。gate 关闭后是 Cost；它不提供更低 TTFT 的数学保证。

## 4. 文件级实现清单

| 文件 | 后续改动与边界 |
|---|---|
| `src/inferscale/config.py` | 新增 prefix_v2 枚举及三个参数，保留旧默认值，验证有限数与范围。 |
| `src/inferscale/features.py` | 独立 SuccessPrefixIndex；原 PrefixIndex 不改，过期重置、淘汰、代际失效单测使用假时钟。 |
| `src/inferscale/policies.py` | V2 分支、gate/bonus/rank 及 fallback；返回不可变决策快照，保留公共成本项与同分逻辑。 |
| `src/inferscale/ledger.py` | 按策略构造索引，在原锁内取分数与预占；保持生命周期和容量计算。记亲和只沿既有成功完成路径，确保每次成功只学习一次。 |
| `src/inferscale/models.py` | Reservation 尾部增加可选决策分解快照，避免打破已有位置参数调用；不保存随时变化的引用。 |
| `src/inferscale/proxy.py` | routing_decision / attempt_finished 导出同一个快照；不改 ASGI 提交、重试、取消、清理流程。 |
| `src/inferscale/samples.py`、`benchmark/report.py` | 新可选字段缺省 null；关联时保留并核对分解信息。以 attempt 为分布分母，区分首尝试与重试、缺失与真实零。旧样本仍可读取。 |
| `benchmark/__main__.py` | 支持 prefix_v2 run；plan 默认四策略，增加显式策略集合/变体配置生成，保留原生 endpoint；禁止覆盖已有 frozen 目录。 |
| `benchmark/compare.py` | 默认严格路径不变，增加显式 V1/V2 comparison spec，见下节。 |
| `scripts/server_smoke.py` | 更新合法 policy 和 CLI choices，继续实际策略校验。 |
| `tests/unit/test_routing_features.py` 等 | 新增分数/排序/状态边界；保留 V1 回归。 |
| `tests/integration/test_p3_gateway.py`、benchmark CLI / server smoke 测试 | 覆盖 V2 成功、失败/重试/取消及日志到报告的完整链路。 |
| `deploy/P4实验运行.md`、README、打包脚本 | 补 V2 操作与独立实验对比规格示例；正常配置模板入包，不以 runs 里的临时 YAML 作为唯一交付。 |

不修改现有 workloads、冻结轨迹、已有实验结果、vLLM 参数、模型/tokenizer、成本基准或容量。不从真实缓存 metrics 反馈路由。不引入 Redis、后台扫描器或新的取消证据要求。

## 5. 可比较性修订

新增显式 comparison spec，记录实验名称、variant_id 和每个变体完整 routing 配置。最小两组：`v1_g025`、`v2_g050`。variant_id 是不同实验配置的身份，不能只用 policy 区分未来的 gamma 消融。

1. 默认 compare 仍按完整公共条件严格分组，旧工具行为不静默放宽。
2. 显式规格只允许 routing.policy、gamma、prefix_hit_increment、prefix_history_retention、prefix_load_soft_limit 为处理差异；每个 run 的配置必须精确匹配其变体定义，不允许通配符。
3. 模型、硬件/后端参数、capacity、cost_reference、其他权重、prefix 长度/TTL/条目上限、trace hash、缓存准备、客户端负载/超时/SLO 和源码版本仍必须一致。不能笼统删除 routing 或 source_hashes。
4. 在同一新版源码上跑 V1 和 V2，记录完整参数与规格摘要。旧数据保持原样且仍可读，但不自动与新版做收益合并。新增无行为影响的默认字段由新版同批实验一致序列化，无需修改旧 manifest。
5. 同 variant 同轮次不能重复；报告按 variant 输出每轮和均值/范围。复用所有现有样本完整性及发送器受限检查。
6. 旧 request/attempt schema 不变更必填项，可选字段缺失时为空。新的 comparison spec 使用独立版本，不篡改旧实验 schema_version。

对照测试必须证明：显式规格中的 V1/V2 能同组；capacity、trace、模型、源码任一变化仍不能同组；未声明的 gamma 变化被拒绝；同一 policy 的两个变体不会被误判为重复轮次。

## 6. 实施顺序与验收

| 阶段 | 工作 | 完成条件 |
|---|---|---|
| V2-0 基线 | 核对目标仓库/本地差异，确认未覆盖用户改动；冻结本节约定 | 记录实际 commit 或源码摘要；现有测试基线明确。157 是上轮已通过数量，本轮未重跑。 |
| V2-1 纯算法 | 先补递推与打分边界测试，再改 config/features/policies | 递推第六次饱和、TTL 等号边界、无时间衰减、独立 Worker、None 前缀、淘汰/代际、gate 边界、fallback、gamma=0 与 Cost 等价。 |
| V2-2 网关接入 | ledger、决策快照、成功学习、日志/样本 | V1 不变；失败 A→成功 B 仅 B 更新一次；失败/取消/超时不学习；旧代际完成不复活条目；日志公式与决策时状态一致。 |
| V2-3 实验兼容 | choices/smoke/plan、显式 comparison spec、报告字段 | 默认仍 36 轮；V2 配置能启动且实际 policy 正确；旧报告可读；V1/V2 可按声明比较，关键环境差异仍拦截。 |
| V2-4 本地交付 | 全量 pytest、Ruff、锁文件检查、包内配置/CPU socket 冒烟 | 出具实际结果与变更文件；无 GPU 不报告性能收益。 |
| V2-5 最小 GPU 对照 | Hot Prefix 同版 V1/V2 各三轮 | 同冻结轨迹/准备过程，随机化或成对交错顺序，保留逐轮终态和分布；再决定是否扩大实验。 |

额外边界：同时构造 q=0、接近阈值、等于阈值及容量已满；满载先被过滤。构造高 affinity 但高成本场景，确认 bonus 不保证胜出。gamma=0 等价性使用相同负载与相同 tie-break 初态。确认旧 V1 成功但 usage 不可信时的亲和行为不被顺带改变。

## 7. 最小实验与停止条件

第一步仍只运行 Hot Prefix 的 V1 gamma=0.25 与 V2 gamma=0.5 各三轮，共六轮。同一新版二进制/源码、同一冻结 trace、相同 seed warmup 和 fresh_seeded 流程。若已有历史 V1 三轮，不直接抵扣新版本对照。

指标：逐轮成功/拒绝/失败/not_sent 分母、窗口吞吐、TTFT/E2E P95、Worker 分布；决策 q、affinity、gate、effective_gamma、bonus 的分布/饱和比例；真实 cache delta 有有效计数器时才报告。首末缓存差分仍是 cohort 观察，不能当请求级命中标签。

若 q 始终很低或两侧 affinity 很快饱和，先解释机制是否被当前负载触发，不改 frozen trace 或容量来追求正收益。若收益不稳定，保留负结果并停止扩展，不重跑全部 P4。

若要解释收益来自哪个机制，可在同一 gamma 下补“V1 二值 + gate”和“连续分数无 gate”等小规模消融，并登记独立 variant；首版不要求完整参数扫描。若要对外声称 V2 普遍更好，则补 Shared Prefix 成对对照，至少双方各三轮；只有 V2 三轮不能构成反例验证。Mixed 及其他负载结论仍限于已有数据，不外推。

以上为冻结设计约定。后续用户已授权实现，V2-0 至 V2-4 的实际改动与验证记录见实现与验收文档；下一步仅由操作者按六轮说明执行 V2-5，本次交付未运行 GPU 实验。
