import json

import httpx
import pytest

from inferscale.benchmark.report import decision_statistics, join_attempts
from inferscale.benchmark.runner import run_trace
from inferscale.config import RoutingConfig
from inferscale.proxy import ProxyResponse
from inferscale.testing.worker import FakeSettings
from tests.integration.test_benchmark_runner import trace
from tests.support import cluster, settled
from tests.unit.test_proxy_lifecycle import BytesStream, drive, prepared


@pytest.mark.parametrize("scenario", ["before-content", "after-content", "cancel", "timeout"])
async def test_v2_retry_failure_cancel_timeout_and_snapshots(scenario):
    fake = FakeSettings(
        worker_id="worker-0",
        scenario=scenario if scenario in {"before-content", "after-content"} else "success",
        first_delay=1 if scenario in {"cancel", "timeout"} else 0,
    )
    options = {"overall_timeout_seconds": 0.04} if scenario == "timeout" else {}
    async with cluster(
        fake, routing={"policy": "prefix_v2", "prefix_tokens": 4, "gamma": 0.5}, **options
    ) as (app, url, a, b):
        events = []
        app.state.telemetry.log = lambda **fields: events.append(fields)
        rows, _, _ = await run_trace(
            trace(duration=0.04 if scenario == "cancel" else 0.2),
            url,
            "fake-model",
            "prefix_v2",
            drain=0.02 if scenario == "cancel" else 1,
        )
        await settled(app, a, b)
        joined, attempts, evidence = join_attempts(rows, "\n".join(json.dumps(e) for e in events))
        assert evidence["complete"]
        assert attempts[0].decision.queue_ratio == 0
        assert attempts[0].decision.load_gate == 1
        if scenario == "before-content":
            assert joined[0].attempt_count == 2 and joined[0].outcome == "succeeded"
            entries = list(app.state.ledger.prefixes.entries.items())
            assert len(entries) == 1 and entries[0][0][0] == "worker-1"
            assert entries[0][1].score == 0.25
            assert decision_statistics(attempts)["retry"]["attempt_count"] == 1
        else:
            assert not app.state.ledger.prefixes.entries
            assert joined[0].outcome != "succeeded"
        starts = [e for e in events if e["event"] == "routing_decision"]
        ends = {e["attempt_id"]: e for e in events if e["event"] == "attempt_finished"}
        for start in starts:
            assert start["decision"] == ends[start["attempt_id"]]["decision"]
        altered = json.loads(json.dumps(events))
        next(e for e in altered if e["event"] == "attempt_finished")["decision"]["queue_ratio"] = (
            0.5
        )
        with pytest.raises(ValueError, match="snapshot mismatch"):
            join_attempts(rows, "\n".join(json.dumps(e) for e in altered))


@pytest.mark.parametrize("policy", ["prefix", "prefix_v2"])
async def test_success_without_usage_still_learns_history(settings, policy):
    app = prepared(
        settings.model_copy(update={"routing": RoutingConfig(policy=policy, prefix_tokens=4)})
    )

    async def transport(request):
        return httpx.Response(
            200, headers={"content-type": "text/event-stream"}, stream=BytesStream()
        )

    async def send(message):
        pass

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        app.state.inference_client = client
        await drive(ProxyResponse(app, chat=True), send)
    assert app.state.predictor.updates == 0
    assert len(app.state.ledger.prefixes.entries) == 1
    if policy == "prefix_v2":
        assert next(iter(app.state.ledger.prefixes.entries.values())).score == 0.25
