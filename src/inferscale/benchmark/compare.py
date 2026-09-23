import csv
import hashlib
import json
from collections import defaultdict
from statistics import mean


def compare(directories, output):
    groups = defaultdict(list)
    seen = set()
    for directory in directories:
        manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
        summary = json.loads((directory / "summary.json").read_text(encoding="utf-8"))
        if manifest["run_id"] in seen:
            raise ValueError("duplicate run supplied")
        seen.add(manifest["run_id"])
        config = dict(manifest["config"])
        config["routing"] = {k: v for k, v in config["routing"].items() if k != "policy"}
        comparable = {
            k: manifest[k]
            for k in (
                "trace_sha256",
                "environment",
                "source_hashes",
                "dependencies",
                "python",
                "cache_condition",
                "mode",
                "capacity",
                "client_timeout",
                "drain_seconds",
                "lag_tolerance",
                "slo",
            )
        }
        comparable["config"] = config
        group = hashlib.sha256(json.dumps(comparable, sort_keys=True).encode()).hexdigest()[:12]
        groups[(manifest["workload"], group, manifest["policy"])].append(
            (manifest, summary, directory)
        )
    output.mkdir(parents=True, exist_ok=False)
    rows = []
    for (workload, group, policy), entries in sorted(groups.items()):
        rounds = [m["round"] for m, _, _ in entries]
        if len(set(rounds)) != len(rounds):
            raise ValueError("duplicate round number for same comparison group and policy")
        usable = [s for _, s, _ in entries if not s["comparison_exclusion_reasons"]]
        row = {
            "workload": workload,
            "comparison_group": group,
            "policy": policy,
            "total_rounds": len(entries),
            "usable_rounds": len(usable),
            "at_least_three_usable_rounds": len(usable) >= 3,
        }
        for key, field in [
            ("throughput", "request_throughput"),
            ("ttft_p95", "ttft_seconds"),
            ("e2e_p95", "e2e_seconds"),
        ]:
            values = [s[field] if key == "throughput" else s[field]["p95"] for s in usable]
            values = [v for v in values if v is not None]
            row[key + "_mean"] = mean(values) if values else None
            row[key + "_min"] = min(values) if values else None
            row[key + "_max"] = max(values) if values else None
        row["round_outcomes"] = [s["outcomes"] for _, s, _ in entries]
        rows.append(row)
    (output / "comparison.json").write_text(json.dumps(rows, indent=2), encoding="utf-8")
    with (output / "comparison.csv").open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]) if rows else [])
        writer.writeheader()
        writer.writerows(rows)
    lines = [
        "# 策略对比",
        "",
        "仅同一 comparison_group 内可比较；每策略至少三轮有效数据。"
        "不同版本/轨迹/缓存条件自动分组。",
        "",
        "| 负载 | 组 | 策略 | 有效/总轮数 | 吞吐均值 | TTFT P95 均值 | E2E P95 均值 |",
        "|---|---|---|---|---|---|---|",
    ]
    for row in rows:
        lines.append(
            f"| {row['workload']} | {row['comparison_group']} | {row['policy']} | "
            f"{row['usable_rounds']}/{row['total_rounds']} | {row['throughput_mean']} | "
            f"{row['ttft_p95_mean']} | {row['e2e_p95_mean']} |"
        )
    lines.extend(
        ["", "均值和范围见 JSON/CSV；逐轮失败分母保留在 round_outcomes。未自动宣称任何策略有收益。"]
    )
    (output / "comparison.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
