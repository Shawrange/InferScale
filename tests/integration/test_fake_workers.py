import asyncio
import json
import socket
from contextlib import asynccontextmanager, contextmanager, suppress

import httpx
import pytest
import uvicorn

from inferscale.config import Settings
from inferscale.lifecycle import request_scope
from inferscale.main import create_app, probe_once
from inferscale.models import RequestContext
from inferscale.testing.worker import FakeSettings, create_fake_app


class TestServer(uvicorn.Server):
    __test__ = False

    @contextmanager
    def capture_signals(self):
        yield


@asynccontextmanager
async def serve(app):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(128)
    port = sock.getsockname()[1]
    server = TestServer(uvicorn.Config(app, log_level="error", lifespan="on"))
    task = asyncio.create_task(server.serve(sockets=[sock]))
    try:
        async with asyncio.timeout(5):
            while not server.started:
                if task.done():
                    await task
                    raise RuntimeError("test server stopped before startup")
                await asyncio.sleep(0.01)
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        try:
            await asyncio.wait_for(asyncio.shield(task), 5)
        except TimeoutError:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
            raise
        finally:
            sock.close()


def payload(stream=True):
    return {
        "model": "fake-model",
        "messages": [{"role": "user", "content": "test"}],
        "stream": stream,
    }


def events(response):
    return [line[6:] for line in response.text.splitlines() if line.startswith("data: ")]


@pytest.mark.parametrize("scenario", ["success", "before-content", "after-content", "empty"])
async def test_real_socket_stream_scenarios(scenario):
    app = create_fake_app(FakeSettings(scenario=scenario))
    async with serve(app) as url, httpx.AsyncClient(trust_env=False) as client:
        response = await client.post(f"{url}/v1/chat/completions", json=payload())
        assert response.status_code == 200
        parts = events(response)
        text = "".join(
            json.loads(part)["choices"][0]["delta"].get("content", "")
            for part in parts
            if part != "[DONE]"
        )
        assert ("[DONE]" in parts) == (scenario in {"success", "empty"})
        assert (
            text
            == {
                "success": "Hello 世界",
                "empty": "",
                "before-content": "",
                "after-content": "Hello",
            }[scenario]
        )
        history = (await client.get(f"{url}/_test/requests")).json()
        assert history["counters"]["active"] == 0
        assert history["counters"]["total"] == 1


@pytest.mark.parametrize("status", [502, 503, 504])
async def test_injected_http_failures(status):
    app = create_fake_app(FakeSettings(scenario="http-error", error_status=status))
    async with serve(app) as url, httpx.AsyncClient(trust_env=False) as client:
        response = await client.post(f"{url}/v1/chat/completions", json=payload())
        assert response.status_code == status
        assert app.state.counters["active"] == 0


async def test_disconnect_during_first_content_wait_clears_fake_active():
    app = create_fake_app(FakeSettings(first_delay=30))
    async with serve(app) as url, httpx.AsyncClient(trust_env=False, timeout=2) as client:
        async with client.stream("POST", f"{url}/v1/chat/completions", json=payload()) as response:
            async for line in response.aiter_lines():
                if line.startswith("data: "):
                    assert "assistant" in line
                    break
        async with asyncio.timeout(2):
            await app.state.idle.wait()
        assert app.state.counters["active"] == 0
        assert app.state.counters["cancelled"] == 1


async def test_two_live_workers_rr_accounting_and_health(settings):
    fake_a = create_fake_app(FakeSettings(worker_id="worker-0"))
    fake_b = create_fake_app(FakeSettings(worker_id="worker-1"))
    async with serve(fake_a) as a, serve(fake_b) as b:
        values = settings.model_dump(mode="json")
        values["workers"][0]["endpoint"] = a
        values["workers"][1]["endpoint"] = b
        app = create_app(Settings.model_validate(values))
        async with serve(app) as gateway, httpx.AsyncClient(trust_env=False) as client:
            assert (await client.get(f"{gateway}/health/ready")).json()["ready_workers"] == 2
            registry, ledger = app.state.registry, app.state.ledger
            chosen = []
            for i in range(4):
                with request_scope(ledger, RequestContext(f"r-{i}", "fake-model")) as lease:
                    attempt = ledger.select_and_reserve(lease, 512)
                    chosen.append(attempt.worker_id)
                    worker = next(
                        w for w in registry.snapshots() if w.worker_id == attempt.worker_id
                    )
                    response = await client.post(
                        f"{worker.endpoint}/v1/chat/completions",
                        json=payload(False),
                        headers={
                            "x-request-id": lease.request.request_id,
                            "x-attempt-id": attempt.attempt_id,
                        },
                    )
                    assert response.json()["choices"][0]["message"]["content"] == "Hello 世界"
            assert chosen == ["worker-0", "worker-1"] * 2
            assert ledger.snapshot()["active_requests"] == ledger.snapshot()["active_attempts"] == 0
            assert fake_a.state.counters["total"] == fake_b.state.counters["total"] == 2
            metrics = (await client.get(f"{gateway}/metrics")).text
            assert "inferscale_active_requests 0" in metrics
            # P2 exposes the actual generation proxy.
            assert (
                await client.post(f"{gateway}/v1/chat/completions", json=payload())
            ).status_code == 200


async def test_probe_failure_is_not_healthy(settings):
    app = create_app(settings)

    async def reject(request):
        return httpx.Response(503)

    async with httpx.AsyncClient(transport=httpx.MockTransport(reject)) as client:
        await probe_once(app.state.registry, client)
    assert app.state.registry.candidates(settings.model.name) == ()


@pytest.mark.parametrize("stream", [False, True])
async def test_completion_api(stream):
    async with (
        serve(create_fake_app(FakeSettings())) as url,
        httpx.AsyncClient(trust_env=False) as client,
    ):
        response = await client.post(
            f"{url}/v1/completions",
            json={
                "model": "fake-model",
                "prompt": "test",
                "stream": stream,
                "max_tokens": 1,
            },
        )
        if stream:
            assert events(response)[-1] == "[DONE]"
        else:
            assert response.json()["choices"][0]["text"] == "Hello"
            assert response.json()["usage"]["completion_tokens"] == 1
