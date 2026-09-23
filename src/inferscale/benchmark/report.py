import json
import math
from collections import Counter, defaultdict
from pathlib import Path

from inferscale.samples import AttemptSample, RequestSample


def distribution(values):
    ordered = sorted(values)
    if not ordered:
        return {"count": 0, "p50": None, "p95": None}

    def quantile(q):
        position = (len(ordered) - 1) * q
        lower = int(position)
        upper = min(lower + 1, len(ordered) - 1)
        return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)

    return {"count": len(values), "p50": quantile(0.5), "p95": quantile(0.95)}


def join_attempts(rows, log_text):
    """Client timings never mix with gateway clock; correlate via bounded UUID header."""
    ids = {r.request_id for r in rows}
    starts, ends, terminals = {}, {}, {}
    for line in log_text.splitlines():
        if not line.startswith("{"):
            continue
        event = json.loads(line)
        cid = event.get("client_request_id")
        if cid not in ids:
            continue
        kind = event.get("event")
        target, key = (None, None)
        if kind == "routing_decision":
            target, key = starts, event["attempt_id"]
        elif kind == "attempt_finished":
            target, key = ends, event["attempt_id"]
        elif kind == "request_finished":
            target, key = terminals, cid
        if target is not None:
            if key in target:
                raise ValueError(f"duplicate gateway event: {kind}/{key}")
            target[key] = event
    if set(ends) - set(starts):
        raise ValueError("attempt terminal without routing decision")
    attempts, by_request = [], defaultdict(list)
    for aid, start in starts.items():
        by_request[start["client_request_id"]].append(start)
        if aid not in ends:
            continue
        end = ends[aid]
        if start.get("policy") == "prefix_v2" and start.get("decision") is None:
            raise ValueError("V2 attempt missing routing decision snapshot")
        if start.get("decision") != end.get("decision"):
            raise ValueError(f"attempt decision snapshot mismatch: {aid}")
        if start.get("decision") is not None:
            d = start["decision"]
            if (
                start.get("policy") != "prefix_v2"
                or not math.isclose(
                    d["final_rank"], d["base_score"] - d["affinity_bonus"], abs_tol=1e-12
                )
                or not math.isclose(
                    d["affinity_bonus"], d["effective_gamma"] * start["affinity"], abs_tol=1e-12
                )
            ):
                raise ValueError("inconsistent V2 routing decomposition")
        for field in (
            "client_request_id",
            "request_id",
            "worker_id",
            "attempt_number",
            "reserved_cost",
        ):
            if start[field] != end[field]:
                raise ValueError(f"attempt identity mismatch: {aid}/{field}")
        attempts.append(
            AttemptSample(
                request_id=start["client_request_id"],
                gateway_request_id=start["request_id"],
                attempt_id=aid,
                worker_id=start["worker_id"],
                attempt_number=start["attempt_number"],
                reserved_cost=start["reserved_cost"],
                outcome=end["outcome"],
                policy=start["policy"],
                score=start.get("score"),
                affinity=start.get("affinity"),
                seconds=end.get("seconds"),
                decision=start.get("decision"),
            )
        )
    joined = []
    missing = []
    for row in rows:
        records = sorted(by_request[row.request_id], key=lambda e: e["attempt_number"])
        if [e["attempt_number"] for e in records] != list(range(1, len(records) + 1)) or len(
            records
        ) > 2:
            raise ValueError("invalid attempt sequence")
        terminal = terminals.get(row.request_id)
        complete = terminal is not None and all(e["attempt_id"] in ends for e in records)
        if row.outcome == "not_sent":
            if records or terminal:
                raise ValueError("not_sent request has gateway activity")
            joined.append(row)
            continue
        if not complete:
            missing.append(row.request_id)
            joined.append(row.model_copy(update={"attempt_count": None, "worker_id": None}))
            continue
        if row.gateway_request_id and row.gateway_request_id != terminal["request_id"]:
            raise ValueError("client/server request identity mismatch")
        if any(e["request_id"] != terminal["request_id"] for e in records):
            raise ValueError("attempt belongs to different gateway request")
        joined.append(
            row.model_copy(
                update={
                    "attempt_count": len(records),
                    "worker_id": records[-1]["worker_id"] if records else None,
                    "gateway_request_id": terminal["request_id"],
                }
            )
        )
    return (
        joined,
        attempts,
        {
            "complete": not missing,
            "missing_request_ids": missing,
            "unfinished_attempt_ids": sorted(set(starts) - set(ends)),
        },
    )


def summarize(rows, duration, *, ttft_slo=1.0, e2e_slo=10.0):
    if duration <= 0 or len({r.request_id for r in rows}) != len(rows):
        raise ValueError("positive window and unique request IDs required")
    counts = Counter(r.outcome for r in rows)
    sent = len(rows) - counts["not_sent"]
    assert sent == sum(counts[k] for k in ("succeeded", "rejected", "failed", "cancelled"))
    success = [r for r in rows if r.outcome == "succeeded"]
    window = [r for r in success if 0 <= r.terminal_at < duration]
    ttft = [r.first_content_at - r.sent_at for r in success if r.first_content_at is not None]
    e2e = [r.terminal_at - r.sent_at for r in success]
    tpot = [
        (r.last_content_at - r.first_content_at) / (r.output_tokens - 1)
        for r in success
        if r.first_content_at is not None and r.output_tokens is not None and r.output_tokens > 1
    ]
    known_window_usage = all(r.output_tokens is not None for r in window)
    good = [
        r
        for r in window
        if r.first_content_at is not None
        and r.first_content_at - r.sent_at <= ttft_slo
        and r.terminal_at - r.sent_at <= e2e_slo
    ]
    failures = [
        r.terminal_at - r.sent_at
        for r in rows
        if r.sent_at is not None and r.outcome != "succeeded"
    ]
    return {
        "planned": len(rows),
        "sent": sent,
        "outcomes": {
            key: counts[key] for key in ("not_sent", "succeeded", "rejected", "failed", "cancelled")
        },
        "error_subtypes": dict(Counter(r.error_subtype for r in rows if r.error_subtype)),
        "window_seconds": duration,
        "window_successes": len(window),
        "drain_successes": len(success) - len(window),
        "request_throughput": len(window) / duration,
        "output_token_throughput": sum(r.output_tokens for r in window) / duration
        if known_window_usage
        else None,
        "window_usage_complete": known_window_usage,
        "success_usage_coverage": sum(r.output_tokens is not None for r in success) / len(success)
        if success
        else None,
        "rejected_per_sent": counts["rejected"] / sent if sent else None,
        "failed_per_sent": counts["failed"] / sent if sent else None,
        "rejected_per_planned": counts["rejected"] / len(rows) if rows else None,
        "failed_per_planned": counts["failed"] / len(rows) if rows else None,
        "ttft_seconds": distribution(ttft),
        "e2e_seconds": distribution(e2e),
        "failed_or_cancelled_or_rejected_seconds": distribution(failures),
        "tpot_estimate_seconds": distribution(tpot),
        "chunk_interval_seconds": distribution([v for r in success for v in r.chunk_intervals]),
        "scheduled_to_sent_seconds": distribution(
            [r.sent_at - r.scheduled_at for r in rows if r.sent_at is not None]
        ),
        "slo": {"ttft_seconds": ttft_slo, "e2e_seconds": e2e_slo, "goodput": len(good) / duration},
        "attempt_count": sum(r.attempt_count for r in rows)
        if all(r.attempt_count is not None for r in rows)
        else None,
        "final_worker_distribution": dict(Counter(r.worker_id for r in success if r.worker_id)),
        "length_finished_successes": sum(r.finish_reason == "length" for r in success),
    }


def cache_delta(before, after):
    def parse(text, metric):
        values = {}
        for line in text.splitlines():
            if line.startswith(metric + "{") or line.startswith(metric + " "):
                key, value = line.rsplit(" ", 1)
                values[key] = float(value)
        return values

    result = {}
    for name in ("queries", "hits"):
        metric = f"vllm:prefix_cache_{name}_total"
        first, last = parse(before, metric), parse(after, metric)
        born_before = parse(before, f"vllm:prefix_cache_{name}_created")
        born_after = parse(after, f"vllm:prefix_cache_{name}_created")
        if (
            not first
            or first.keys() != last.keys()
            or born_before != born_after
            or any(not math.isfinite(v) or v < 0 for v in [*first.values(), *last.values()])
            or any(last[k] < first[k] for k in first)
        ):
            return {"valid": False, "reason": "missing_series_or_counter_reset", "hit_ratio": None}
        result[name] = sum(last[k] - first[k] for k in first)
    if result["hits"] > result["queries"]:
        return {"valid": False, "reason": "hits_exceed_queries", "hit_ratio": None}
    return {
        "valid": True,
        "unit": "tokens (verify backend HELP)",
        **result,
        "hit_ratio": result["hits"] / result["queries"] if result["queries"] else None,
    }


def decision_statistics(attempts):
    """Attempt denominator; null is not zero. Sequence is retained separately."""
    result = {}
    for label, selected in (
        ("all", attempts),
        ("first", [a for a in attempts if a.attempt_number == 1]),
        ("retry", [a for a in attempts if a.attempt_number > 1]),
    ):
        fields = {"affinity": [a.affinity for a in selected if a.affinity is not None]}
        for field in (
            "queue_ratio",
            "load_gate",
            "effective_gamma",
            "affinity_bonus",
            "base_score",
            "final_rank",
        ):
            fields[field] = [getattr(a.decision, field) for a in selected if a.decision is not None]
        result[label] = {
            "attempt_count": len(selected),
            "fields": {
                name: {**distribution(values), "missing": len(selected) - len(values)}
                for name, values in fields.items()
            },
            "affinity_saturation_fraction": sum(v == 1 for v in fields["affinity"])
            / len(fields["affinity"])
            if fields["affinity"]
            else None,
        }
    return result


def write_report(directory: Path, *, log_text=None):
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    raw = directory / "requests.raw.jsonl"
    rows = [
        RequestSample.model_validate_json(line)
        for line in (raw if raw.exists() else directory / "requests.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    if manifest.get("status") != "completed":
        raise ValueError(
            "incomplete run: preserve raw records but do not produce a completed report"
        )
    expected = manifest["planned_count"]
    if len(rows) != expected or {r.trace_index for r in rows} != set(range(expected)):
        raise ValueError("raw samples do not cover every planned trace item exactly once")
    attempts, evidence = [], {"complete": False, "reason": "gateway_log_not_supplied"}
    if log_text is not None:
        rows, attempts, evidence = join_attempts(rows, log_text)
        routing = manifest["config"]["routing"]
        for attempt in attempts:
            if attempt.decision is None:
                continue
            d = attempt.decision
            expected_gate = max(0.0, 1.0 - d.queue_ratio / routing["prefix_load_soft_limit"])
            expected_score = d.final_rank + routing["cost_weight"] * (
                attempt.reserved_cost / routing["cost_reference"]
            )
            if not (
                math.isclose(d.load_gate, expected_gate, abs_tol=1e-12)
                and math.isclose(d.effective_gamma, routing["gamma"] * d.load_gate, abs_tol=1e-12)
                and attempt.score is not None
                and math.isclose(attempt.score, expected_score, abs_tol=1e-12)
            ):
                raise ValueError("routing decision disagrees with run configuration")
        (directory / "requests.jsonl").write_text(
            "".join(r.model_dump_json() + "\n" for r in rows), encoding="utf-8"
        )
    (directory / "attempts.jsonl").write_text(
        "".join(r.model_dump_json() + "\n" for r in attempts), encoding="utf-8"
    )
    summary = summarize(
        rows, manifest["duration"], ttft_slo=manifest["slo"]["ttft"], e2e_slo=manifest["slo"]["e2e"]
    )
    summary["attempt_evidence"] = evidence
    summary["routing_decisions"] = decision_statistics(attempts)
    summary["routing_sequence"] = [
        {
            "attempt_id": a.attempt_id,
            "worker_id": a.worker_id,
            "attempt_number": a.attempt_number,
            "affinity": a.affinity,
            "decision": a.decision.model_dump() if a.decision else None,
        }
        for a in attempts
    ]
    summary["cache_cohort_deltas"] = {}
    for before in directory.glob("before-worker*.prom"):
        after = directory / before.name.replace("before-", "after-drain-")
        if after.exists():
            summary["cache_cohort_deltas"][before.stem] = cache_delta(
                before.read_text(), after.read_text()
            )
    reasons = []
    if manifest["mode"] != "open_loop":
        reasons.append("closed_loop_is_smoke_only")
    if manifest["load_generator_limited"]:
        reasons.append("load_generator_limited")
    if not evidence["complete"]:
        reasons.append("incomplete_attempt_evidence")
    if not manifest.get("environment"):
        reasons.append("missing_environment_record")
    if manifest.get("synthetic"):
        reasons.append("synthetic_backend")
    if manifest.get("cache_condition") != "fresh_seeded":
        reasons.append("uncontrolled_cache_state")
    if any(a.policy != manifest["policy"] for a in attempts):
        reasons.append("attempt_policy_mismatch")
    summary["comparison_exclusion_reasons"] = reasons
    (directory / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    lines = [
        "# InferScale 单轮报告",
        "",
        f"策略：{manifest['policy']}；负载：{manifest['workload']}；模式：{manifest['mode']}",
        "",
        f"计划 {summary['planned']}；发送 {summary['sent']}；终态 {summary['outcomes']}",
        f"窗口成功 {summary['window_successes']}；drain 后成功 {summary['drain_successes']}。",
        "",
        "| 指标 | 结果 |",
        "|---|---|",
        f"| 窗口成功吞吐 req/s | {summary['request_throughput']:.4f} |",
        f"| 成功 TTFT P50/P95 秒 | {summary['ttft_seconds']} |",
        f"| 成功 E2E P50/P95 秒 | {summary['e2e_seconds']} |",
        f"| TPOT 估计秒（不等于逐 token ITL） | {summary['tpot_estimate_seconds']} |",
        f"| 输出 token 吞吐（未知时 null） | {summary['output_token_throughput']} |",
        "",
        f"自动排除理由：{reasons or '无；仍须核对版本、缓存条件与至少三轮重复'}。",
        "缓存差分是 before→after-drain 的 cohort 观察，不是精确测量窗口差分。",
        "亲和命中不等于后端缓存命中；chunk 间隔不等于 token 间隔。详情和分母见 summary.json。",
    ]
    (directory / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return summary
