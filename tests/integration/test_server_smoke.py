import pytest

from scripts.server_smoke import verify
from tests.support import cluster, settled


@pytest.mark.parametrize("policy", ["cost", "prefix_v2"])
async def test_packaged_smoke_with_remote_tokenizer_over_sockets(policy):
    async with cluster(tokenizer_mode="vllm", routing={"policy": policy}) as (app, url, a, b):
        workers = [w.endpoint for w in app.state.registry.snapshots()]
        report = await verify(url, workers, "fake-model", expected_policy=policy)
        assert report["gateway_policy"] == report["expected_policy"] == policy
        assert report["protocol_usage_passed"]
        assert len(report["checks"]) == 12
        assert not report["gpu_cancel_verified"]
        await settled(app, a, b)
        assert app.state.predictor.updates == 4


async def test_policy_mismatch_fails_before_generation():
    async with cluster() as (app, url, a, b):
        workers = [w.endpoint for w in app.state.registry.snapshots()]
        with pytest.raises(ValueError, match="expected prefix, got round_robin"):
            await verify(url, workers, "fake-model", expected_policy="prefix")
        assert a.state.counters["total"] == b.state.counters["total"] == 0
