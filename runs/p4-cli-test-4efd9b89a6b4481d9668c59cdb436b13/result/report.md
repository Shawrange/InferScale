# InferScale 单轮报告

策略：prefix_v2；负载：mixed；模式：open_loop

计划 2；发送 2；终态 {'not_sent': 0, 'succeeded': 2, 'rejected': 0, 'failed': 0, 'cancelled': 0}
窗口成功 2；drain 后成功 0。

| 指标 | 结果 |
|---|---|
| 窗口成功吞吐 req/s | 6.6667 |
| 成功 TTFT P50/P95 秒 | {'count': 2, 'p50': 0.01578358095139265, 'p95': 0.01777851665392518} |
| 成功 E2E P50/P95 秒 | {'count': 2, 'p50': 0.0214988486841321, 'p95': 0.024626402463763953} |
| TPOT 估计秒（不等于逐 token ITL） | {'count': 2, 'p50': 0.001109108328819275, 'p95': 0.001172388345003128} |
| 输出 token 吞吐（未知时 null） | 20.0 |

自动排除理由：['missing_environment_record', 'synthetic_backend']。
缓存差分是 before→after-drain 的 cohort 观察，不是精确测量窗口差分。
亲和命中不等于后端缓存命中；chunk 间隔不等于 token 间隔。详情和分母见 summary.json。
