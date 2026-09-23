import asyncio
import json
from contextlib import suppress

import httpx
import pytest

from inferscale.testing.worker import FakeSettings
from tests.integration.test_fake_workers import events, payload
from tests.support import cluster, settled


@pytest.mark.parametrize("chat", [True, False])
@pytest.mark.parametrize("stream", [True, False])
async def test_generation_paths(chat, stream):
    async with cluster() as (app, url, a, b), httpx.AsyncClient(trust_env=False) as client:
        body = (
            payload(stream)
            if chat
            else {"model": "fake-model", "prompt": "hello", "stream": stream}
        )
        path = "chat/completions" if chat else "completions"
        response = await client.post(f"{url}/v1/{path}", json=body)
        assert response.status_code == 200, response.text
        if stream:
            assert events(response)[-1] == "[DONE]"
        else:
            assert response.json()["usage"]["completion_tokens"] == 3
            assert app.state.telemetry.first_output_count == 0
        await settled(app, a, b)
        assert app.state.ledger.snapshot()["outcomes"] == {"completed": 1}


async def test_s1_discard_old_role_and_retry_before_commit():
    async with (
        cluster(FakeSettings(worker_id="worker-0", scenario="before-content")) as (app, url, a, b),
        httpx.AsyncClient(trust_env=False) as client,
    ):
        response = await client.post(f"{url}/v1/chat/completions", json=payload())
        assert response.status_code == 200
        assert "worker-0" not in response.text
        assert "worker-1" in response.text and events(response)[-1] == "[DONE]"
        await settled(app, a, b)
        assert a.state.counters["total"] == b.state.counters["total"] == 1
        assert app.state.telemetry.counts["retries"] == 1
        assert app.state.ledger.snapshot()["outcomes"] == {"completed": 1}


async def test_s2_no_retry_after_content():
    async with (
        cluster(FakeSettings(scenario="after-content")) as (app, url, a, b),
        httpx.AsyncClient(trust_env=False) as client,
    ):
        response = await client.post(f"{url}/v1/chat/completions", json=payload())
        assert response.status_code == 200
        assert "Hello" in response.text and "[DONE]" not in response.text
        assert "worker-1" not in response.text
        await settled(app, a, b)
        assert b.state.counters["total"] == 0
        assert app.state.telemetry.counts["partial_failures"] == 1
        assert app.state.ledger.snapshot()["outcomes"] == {"failed": 1}


@pytest.mark.parametrize("scenario", ["http-error", "before-content"])
async def test_s4_two_failures_are_http_error(scenario):
    async with (
        cluster(
            FakeSettings(scenario=scenario), FakeSettings(worker_id="worker-1", scenario=scenario)
        ) as (app, url, a, b),
        httpx.AsyncClient(trust_env=False) as client,
    ):
        response = await client.post(f"{url}/v1/chat/completions", json=payload())
        assert response.status_code == 502
        assert "error" in response.json()
        await settled(app, a, b)
        assert a.state.counters["total"] == b.state.counters["total"] == 1


@pytest.mark.parametrize("after_content", [False, True])
async def test_l2_s5_disconnect_closes_upstream(after_content):
    delays = {"chunk_delay": 30} if after_content else {"first_delay": 30}
    async with cluster(FakeSettings(**delays)) as (app, url, a, b):
        async with httpx.AsyncClient(trust_env=False) as client:

            async def run():
                async with client.stream(
                    "POST", f"{url}/v1/chat/completions", json=payload()
                ) as response:
                    async for line in response.aiter_lines():
                        if after_content and "Hello" in line:
                            return

            task = asyncio.create_task(run())
            if after_content:
                await asyncio.wait_for(task, 2)
            else:
                async with asyncio.timeout(2):
                    for _ in range(200):
                        if a.state.counters["active"]:
                            break
                        await asyncio.sleep(0.01)
                assert a.state.counters["active"] == 1
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
            await settled(app, a, b)
            assert b.state.counters["total"] == 0
            assert a.state.counters["cancelled"] == 1
            assert app.state.ledger.snapshot()["outcomes"] == {"cancelled": 1}


async def test_first_output_timeout_is_not_reset_by_role_and_never_retries():
    async with (
        cluster(FakeSettings(first_delay=1), first_output_timeout_seconds=0.08) as (app, url, a, b),
        httpx.AsyncClient(trust_env=False) as client,
    ):
        response = await client.post(f"{url}/v1/chat/completions", json=payload())
        assert response.status_code == 504
        await settled(app, a, b)
        assert b.state.counters["total"] == 0
        assert app.state.telemetry.first_output_count == 0


async def test_idle_timeout_truncates_without_retry():
    async with (
        cluster(FakeSettings(chunk_delay=1), idle_timeout_seconds=0.08) as (app, url, a, b),
        httpx.AsyncClient(trust_env=False) as client,
    ):
        response = await client.post(f"{url}/v1/chat/completions", json=payload())
        assert response.status_code == 200
        assert "Hello" in response.text and "[DONE]" not in response.text
        await settled(app, a, b)
        assert b.state.counters["total"] == 0
        assert app.state.ledger.snapshot()["outcomes"] == {"timeout": 1}


async def test_empty_completion_does_not_record_ttft():
    async with (
        cluster(FakeSettings(scenario="empty")) as (app, url, a, b),
        httpx.AsyncClient(trust_env=False) as client,
    ):
        response = await client.post(f"{url}/v1/chat/completions", json=payload())
        assert events(response)[-1] == "[DONE]"
        await settled(app, a, b)
        assert app.state.telemetry.first_output_count == 0


@pytest.mark.parametrize(
    "updates,code",
    [
        ({"model": "wrong"}, "unknown_model"),
        ({"tools": []}, "invalid_request"),
        ({"max_tokens": 99999}, "output_limit_exceeded"),
        ({"messages": [{"role": "user", "content": "x" * 5000}]}, "context_limit_exceeded"),
        ({"n": 2}, "invalid_request"),
    ],
)
async def test_invalid_request_never_reaches_worker(updates, code):
    async with cluster() as (app, url, a, b), httpx.AsyncClient(trust_env=False) as client:
        response = await client.post(f"{url}/v1/chat/completions", json=payload() | updates)
        assert response.status_code == 400
        assert response.json()["error"]["code"] == code
        await settled(app, a, b)
        assert a.state.counters["total"] == b.state.counters["total"] == 0


async def test_actual_body_byte_limit_chunked_request():
    async with (
        cluster(max_body_bytes=64) as (app, url, a, b),
        httpx.AsyncClient(trust_env=False) as client,
    ):

        async def chunks():
            yield b"{" + b" " * 40
            yield b" " * 40

        response = await client.post(f"{url}/v1/chat/completions", content=chunks())
        assert response.status_code == 413
        await settled(app, a, b)


async def test_global_capacity_fast_rejection():
    async with (
        cluster(FakeSettings(first_delay=0.2), global_capacity=1) as (app, url, a, b),
        httpx.AsyncClient(trust_env=False) as client,
    ):
        first = asyncio.create_task(client.post(f"{url}/v1/chat/completions", json=payload()))
        for _ in range(100):
            if app.state.ledger.snapshot()["active_requests"]:
                break
            await asyncio.sleep(0.01)
        response = await client.post(f"{url}/v1/chat/completions", json=payload())
        assert response.status_code == 503
        assert response.json()["error"]["code"] == "global_capacity_exceeded"
        assert (await first).status_code == 200
        await settled(app, a, b)


async def test_api_key_not_forwarded(monkeypatch):
    monkeypatch.setenv("INFERSCALE_TEST_KEY", "secret-example")
    async with (
        cluster(api_key_env="INFERSCALE_TEST_KEY") as (app, url, a, b),
        httpx.AsyncClient(trust_env=False) as client,
    ):
        denied = await client.post(f"{url}/v1/chat/completions", json=payload())
        assert denied.status_code == 401
        response = await client.post(
            f"{url}/v1/chat/completions",
            json=payload(),
            headers={"Authorization": "Bearer secret-example"},
        )
        assert response.status_code == 200
        assert "secret-example" not in json.dumps(app.state.ledger.snapshot())
        await settled(app, a, b)


async def test_first_output_deadline_spans_both_attempts():
    async with (
        cluster(
            FakeSettings(scenario="before-content", first_delay=0.12),
            FakeSettings(worker_id="worker-1", first_delay=0.12),
            first_output_timeout_seconds=0.2,
        ) as (app, url, a, b),
        httpx.AsyncClient(trust_env=False) as client,
    ):
        response = await client.post(f"{url}/v1/chat/completions", json=payload())
        assert response.status_code == 504
        await settled(app, a, b)
        assert a.state.counters["total"] == b.state.counters["total"] == 1
        assert app.state.ledger.snapshot()["outcomes"] == {"timeout": 1}


async def test_overall_deadline_stops_continuous_output():
    async with (
        cluster(
            FakeSettings(chunk_delay=0.1),
            overall_timeout_seconds=0.15,
            idle_timeout_seconds=5,
        ) as (app, url, a, b),
        httpx.AsyncClient(trust_env=False) as client,
    ):
        response = await client.post(f"{url}/v1/chat/completions", json=payload())
        assert response.status_code == 200 and "[DONE]" not in response.text
        await settled(app, a, b)
        assert app.state.ledger.snapshot()["outcomes"] == {"timeout": 1}


async def test_all_workers_draining_rejects_without_attempt():
    async with cluster() as (app, url, a, b), httpx.AsyncClient(trust_env=False) as client:
        for worker in app.state.registry.snapshots():
            app.state.registry.set_draining(worker.worker_id)
        response = await client.post(f"{url}/v1/chat/completions", json=payload())
        assert response.status_code == 503
        await settled(app, a, b)
        assert a.state.counters["total"] == b.state.counters["total"] == 0


async def test_nonstream_response_size_is_bounded():
    async with (
        cluster(max_response_bytes=64) as (app, url, a, b),
        httpx.AsyncClient(trust_env=False) as client,
    ):
        response = await client.post(f"{url}/v1/chat/completions", json=payload(False))
        assert response.status_code == 502
        assert response.json()["error"]["code"] == "upstream_response_too_large"
        await settled(app, a, b)
        assert b.state.counters["total"] == 0


async def test_gateway_health_worker_selection_remains_model_specific():
    async with cluster() as (app, url, a, b), httpx.AsyncClient(trust_env=False) as client:
        app.state.registry.set_draining("worker-0")
        response = await client.post(f"{url}/v1/chat/completions", json=payload())
        assert response.status_code == 200
        assert "worker-1" in response.text
        await settled(app, a, b)
        assert a.state.counters["total"] == 0
