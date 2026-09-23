# InferScale 单轮报告

策略：round_robin；负载：mixed；模式：closed_loop

计划 120；发送 113；终态 {'not_sent': 7, 'succeeded': 110, 'rejected': 3, 'failed': 0, 'cancelled': 0}
窗口成功 106；drain 后成功 4。

| 指标 | 结果 |
|---|---|
| 窗口成功吞吐 req/s | 1.7667 |
| 成功 TTFT P50/P95 秒 | {'count': 110, 'p50': 0.09938066080212593, 'p95': 0.13338020732626318} |
| 成功 E2E P50/P95 秒 | {'count': 110, 'p50': 0.6647045044228435, 'p95': 4.315809807088226} |
| TPOT 估计秒（不等于逐 token ITL） | {'count': 110, 'p50': 0.01594226000047081, 'p95': 0.01647065667143347} |
| 输出 token 吞吐（未知时 null） | 233.18333333333334 |

自动排除理由：['closed_loop_is_smoke_only', 'load_generator_limited', 'uncontrolled_cache_state']。
缓存差分是 before→after-drain 的 cohort 观察，不是精确测量窗口差分。
亲和命中不等于后端缓存命中；chunk 间隔不等于 token 间隔。详情和分母见 summary.json。
