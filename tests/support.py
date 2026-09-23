import asyncio
from contextlib import asynccontextmanager

from inferscale.config import Settings, load_settings
from inferscale.main import create_app
from inferscale.testing.worker import FakeSettings, create_fake_app
from tests.integration.test_fake_workers import serve


@asynccontextmanager
async def cluster(a=None, b=None, **overrides):
    worker_a = create_fake_app(a or FakeSettings(worker_id="worker-0"))
    worker_b = create_fake_app(b or FakeSettings(worker_id="worker-1"))
    async with serve(worker_a) as url_a, serve(worker_b) as url_b:
        values = load_settings("configs/local.yaml").model_dump(mode="json")
        values["workers"][0]["endpoint"] = url_a
        values["workers"][1]["endpoint"] = url_b
        values.update(overrides)
        app = create_app(Settings.model_validate(values))
        async with serve(app) as url:
            yield app, url, worker_a, worker_b


async def settled(app, *workers):
    # Observe ASGI request finalization, which can occur just after the last client byte.
    async with asyncio.timeout(3):
        for _ in range(300):
            state = app.state.ledger.snapshot()
            if state["active_requests"] == 0 and all(
                w.state.counters["active"] == 0 for w in workers
            ):
                assert state["active_attempts"] == 0
                assert all(v["reserved_cost"] == 0 for v in state["workers"].values())
                return
            await asyncio.sleep(0.01)
    raise AssertionError("resources did not settle")
