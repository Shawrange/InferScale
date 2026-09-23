import json

import pytest

from inferscale.benchmark.report import join_attempts, summarize
from inferscale.benchmark.runner import run_trace
from inferscale.benchmark.workloads import Item, Trace
from inferscale.testing.worker import FakeSettings
from tests.support import cluster, settled


def trace(count=1, duration=0.2, spacing=0.001):
    return Trace(
        workload="mixed",
        seed=42,
        duration=duration,
        rate=1 / spacing,
        warmup=(),
        requests=tuple(
            Item(offset=i * spacing, group="test", prompt="hello", max_tokens=16)
            for i in range(count)
        ),
    )


async def test_b6_open_loop_capacity_does_not_shift_arrivals():
    async with cluster(FakeSettings(worker_id="worker-0", first_delay=0.05)) as (app, url, a, b):
        plan = trace(count=8)
        rows, details, _ = await run_trace(plan, url, "fake-model", "round_robin", capacity=1)
        assert [r.scheduled_at for r in rows] == [i.offset for i in plan.requests]
        assert sum(r.outcome == "not_sent" for r in rows) >= 5
        assert details["load_generator_limited"]
        await settled(app, a, b)


async def test_real_retry_logs_join_without_double_counting():
    async with cluster(FakeSettings(worker_id="worker-0", scenario="before-content")) as (
        app,
        url,
        a,
        b,
    ):
        events = []
        app.state.telemetry.log = lambda **fields: events.append(fields)
        rows, _, _ = await run_trace(trace(), url, "fake-model", "round_robin")
        await settled(app, a, b)
        joined, attempts, evidence = join_attempts(rows, "\n".join(json.dumps(e) for e in events))
        assert evidence["complete"] and len(attempts) == 2
        assert [a.outcome for a in attempts] == ["failed", "completed"]
        assert joined[0].attempt_count == 2
        assert summarize(joined, 0.2)["outcomes"]["succeeded"] == 1


async def test_drain_cancellation_is_recorded_and_joinable_before_headers():
    async with cluster(FakeSettings(worker_id="worker-0", first_delay=1)) as (app, url, a, b):
        events = []
        app.state.telemetry.log = lambda **fields: events.append(fields)
        rows, _, _ = await run_trace(
            trace(duration=0.04), url, "fake-model", "round_robin", drain=0.02
        )
        assert rows[0].outcome == "cancelled"
        await settled(app, a, b)
        joined, attempts, evidence = join_attempts(rows, "\n".join(json.dumps(e) for e in events))
        assert evidence["complete"] and joined[0].attempt_count == 1
        assert attempts[0].outcome == "cancelled"


async def test_closed_loop_and_policy_guard():
    async with cluster() as (app, url, a, b):
        with pytest.raises(ValueError, match="policy mismatch"):
            await run_trace(trace(), url, "fake-model", "cost")
        assert a.state.counters["total"] == 0
        rows, details, _ = await run_trace(
            trace(4), url, "fake-model", "round_robin", mode="closed_loop", capacity=1
        )
        assert all(r.outcome == "succeeded" for r in rows)
        assert details["mode"] == "closed_loop"
        await settled(app, a, b)


async def test_worker_truncation_is_failure_not_successful_eof():
    async with cluster(FakeSettings(worker_id="worker-0", scenario="after-content")) as (
        app,
        url,
        a,
        b,
    ):
        rows, _, _ = await run_trace(trace(), url, "fake-model", "round_robin")
        assert rows[0].outcome == "failed" and rows[0].error_subtype == "missing_valid_done"
        assert rows[0].output_tokens is None
        await settled(app, a, b)
