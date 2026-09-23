import httpx
import pytest

from inferscale.testing.worker import FakeSettings
from tests.support import cluster, settled


@pytest.mark.parametrize("policy", ["round_robin", "least_load", "cost", "prefix", "prefix_v2"])
@pytest.mark.parametrize("stream", [False, True])
async def test_policy_pipeline_learns_only_success(policy, stream):
    async with cluster(routing={"policy": policy, "prefix_tokens": 8}) as (app, url, a, b):
        async with httpx.AsyncClient() as client:
            response = await client.post(
                url + "/v1/chat/completions",
                json={
                    "model": "fake-model",
                    "messages": [{"role": "user", "content": "shared prefix hello"}],
                    "stream": stream,
                },
            )
            assert response.status_code == 200
        await settled(app, a, b)
        assert app.state.predictor.updates == 1
        assert len(app.state.ledger.prefixes.entries) == 1
        assert app.state.predictor.estimate(25, 64, None).output_estimate < 64


@pytest.mark.parametrize("policy", ["prefix", "prefix_v2"])
async def test_partial_failure_does_not_learn_usage_or_prefix(policy):
    async with cluster(
        FakeSettings(worker_id="worker-0", scenario="after-content"),
        routing={"policy": policy, "prefix_tokens": 8},
    ) as (app, url, a, b):
        async with httpx.AsyncClient() as client:
            response = await client.post(
                url + "/v1/completions",
                json={
                    "model": "fake-model",
                    "prompt": "shared prefix hello",
                    "stream": True,
                },
            )
        assert "[DONE]" not in response.text
        await settled(app, a, b)
        assert app.state.predictor.updates == 0
        assert not app.state.ledger.prefixes.entries
