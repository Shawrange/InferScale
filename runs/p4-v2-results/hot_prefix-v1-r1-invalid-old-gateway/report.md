# InferScale 单轮报告

策略：prefix；负载：hot_prefix；模式：open_loop

计划 660；发送 660；终态 {'not_sent': 0, 'succeeded': 651, 'rejected': 9, 'failed': 0, 'cancelled': 0}
窗口成功 623；drain 后成功 28。

| 指标 | 结果 |
|---|---|
| 窗口成功吞吐 req/s | 10.3833 |
| 成功 TTFT P50/P95 秒 | {'count': 651, 'p50': 0.10327296890318394, 'p95': 0.14489398524165154} |
| 成功 E2E P50/P95 秒 | {'count': 651, 'p50': 2.3029637932777405, 'p95': 4.573178273625672} |
| TPOT 估计秒（不等于逐 token ITL） | {'count': 651, 'p50': 0.01720068655615928, 'p95': 0.01805327189572918} |
| 输出 token 吞吐（未知时 null） | 1490.2833333333333 |

自动排除理由：['incomplete_attempt_evidence']。
缓存差分是 before→after-drain 的 cohort 观察，不是精确测量窗口差分。
亲和命中不等于后端缓存命中；chunk 间隔不等于 token 间隔。详情和分母见 summary.json。
