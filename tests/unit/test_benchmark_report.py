import json

import pytest

from inferscale.benchmark.report import cache_delta, join_attempts, summarize
from inferscale.benchmark.workloads import generate
from inferscale.samples import RequestSample


def sample(name="r", **changes):
    data = dict(
        request_id=name,
        scheduled_at=0,
        sent_at=0,
        first_content_at=0.04,
        last_content_at=0.1,
        terminal_at=0.12,
        output_tokens=4,
        outcome="succeeded",
    )
    data.update(changes)
    return RequestSample(**data)


def test_b1_content_clock_and_tpot():
    summary = summarize([sample()], 1)
    assert summary["ttft_seconds"]["p50"] == pytest.approx(0.04)
    assert summary["e2e_seconds"]["p50"] == pytest.approx(0.12)
    assert summary["tpot_estimate_seconds"]["p50"] == pytest.approx(0.02)


def test_b2_full_denominators_and_timeout_not_double_counted():
    rows = [sample(str(i)) for i in range(5)]
    for name, outcome, error in [
        ("reject", "rejected", "http_429"),
        ("timeout", "failed", "timeout"),
        ("cancel", "cancelled", "drain_deadline"),
    ]:
        rows.append(
            sample(
                name,
                outcome=outcome,
                error_subtype=error,
                output_tokens=None,
                first_content_at=None,
                last_content_at=None,
            )
        )
    rows.extend(
        RequestSample(
            request_id=f"n{i}", scheduled_at=0, terminal_at=0, outcome="not_sent", attempt_count=0
        )
        for i in range(2)
    )
    summary = summarize(rows, 1)
    assert summary["planned"] == 10 and summary["sent"] == 8
    assert summary["outcomes"] == {
        "not_sent": 2,
        "succeeded": 5,
        "rejected": 1,
        "failed": 1,
        "cancelled": 1,
    }
    assert summary["failed_per_sent"] == 1 / 8
    assert summary["failed_per_planned"] == 0.1


def event_log():
    events = []
    for number, worker, outcome in [(1, "a", "failed"), (2, "b", "completed")]:
        base = dict(
            client_request_id="r",
            request_id="gateway-r",
            attempt_id=str(number),
            attempt_number=number,
            worker_id=worker,
            reserved_cost=100,
            policy="cost",
        )
        events.append(dict(base, event="routing_decision"))
        events.append(dict(base, event="attempt_finished", outcome=outcome, seconds=0.01))
    events.append(
        dict(
            event="request_finished",
            client_request_id="r",
            request_id="gateway-r",
            outcome="completed",
        )
    )
    return events


def test_b3_retry_is_one_success_and_two_attempts():
    rows, attempts, evidence = join_attempts(
        [sample()], "\n".join(json.dumps(e) for e in event_log())
    )
    assert evidence["complete"]
    assert rows[0].worker_id == "b"
    assert len(attempts) == rows[0].attempt_count == 2
    assert summarize(rows, 1)["outcomes"]["succeeded"] == 1


def test_missing_or_duplicate_logs_do_not_fabricate_zero_attempts():
    rows, _, evidence = join_attempts([sample()], "")
    assert not evidence["complete"] and rows[0].attempt_count is None
    events = event_log()
    with pytest.raises(ValueError, match="duplicate"):
        join_attempts([sample()], "\n".join(json.dumps(e) for e in events + events))


def test_b4_drain_completion_excluded_from_window_throughput():
    result = summarize([sample(terminal_at=1.1)], 1)
    assert result["outcomes"]["succeeded"] == 1
    assert result["window_successes"] == 0 and result["drain_successes"] == 1
    assert result["request_throughput"] == 0


def test_b5_unknown_usage_not_invented_from_chunks():
    result = summarize([sample(output_tokens=None, chunk_intervals=(0.01, 0.02))], 1)
    assert result["chunk_interval_seconds"]["count"] == 2
    assert result["tpot_estimate_seconds"]["count"] == 0
    assert result["output_token_throughput"] is None


def test_empty_text_and_zero_sent_have_no_fake_ttft_or_rate():
    result = summarize([sample(first_content_at=None, last_content_at=None, output_tokens=0)], 1)
    assert result["ttft_seconds"]["count"] == 0
    row = RequestSample(request_id="n", scheduled_at=0, terminal_at=0, outcome="not_sent")
    result = summarize([row], 1)
    assert result["failed_per_sent"] is None


def test_cache_deltas_and_restart_detection():
    def metrics(q, h, born=1):
        return (
            f'vllm:prefix_cache_queries_total{{engine="0"}} {q}\n'
            f'vllm:prefix_cache_hits_total{{engine="0"}} {h}\n'
            f'vllm:prefix_cache_queries_created{{engine="0"}} {born}\n'
            f'vllm:prefix_cache_hits_created{{engine="0"}} {born}\n'
        )

    result = cache_delta(metrics(100, 50), metrics(200, 125))
    assert result["hit_ratio"] == 0.75
    assert not cache_delta(metrics(100, 50), metrics(1, 0))["valid"]
    assert not cache_delta(metrics(100, 50), metrics(200, 125, 2))["valid"]
    assert cache_delta(metrics(100, 50), metrics(100, 50))["hit_ratio"] is None


@pytest.mark.parametrize("workload", ["mixed", "shared_prefix", "hot_prefix"])
def test_reproducible_arrival_trace_and_output_caps(workload):
    trace = generate(workload, 42, 2, 10)
    assert trace == generate(workload, 42, 2, 10)
    assert trace != generate(workload, 43, 2, 10)
    assert [r.offset for r in trace.requests] == [i / 10 for i in range(20)]
    assert all(r.max_tokens <= 256 for r in trace.requests)
