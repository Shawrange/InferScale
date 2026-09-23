import asyncio
import json

import httpx
import pytest

from inferscale.main import create_app
from inferscale.models import Health, RequestContext
from inferscale.proxy import ProxyResponse
from inferscale.testing.worker import sse


def stream_bytes():
    return b"".join(
        [
            sse(
                {
                    "id": "a",
                    "choices": [{"index": 0, "delta": {"content": "Hello"}, "finish_reason": None}],
                }
            ),
            sse({"id": "a", "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]}),
            sse("[DONE]"),
        ]
    )


class BytesStream(httpx.AsyncByteStream):
    async def __aiter__(self):
        yield stream_bytes()


def prepared(settings):
    app = create_app(settings)
    for worker in app.state.registry.snapshots():
        app.state.registry.record_health(
            worker.worker_id, worker.epoch, True, app.state.registry.clock()
        )
    return app


async def drive(response, send):
    received = False

    async def receive():
        nonlocal received
        if not received:
            received = True
            return {
                "type": "http.request",
                "body": json.dumps(
                    {
                        "model": "fake-model",
                        "messages": [{"role": "user", "content": "hello"}],
                        "stream": True,
                    }
                ).encode(),
                "more_body": False,
            }
        await asyncio.Event().wait()

    await response({"type": "http", "headers": []}, receive, send)


async def test_s3_response_start_send_failure_never_retries(settings):
    app = prepared(settings)
    calls = []

    async def transport(request):
        calls.append(request)
        return httpx.Response(
            200, headers={"content-type": "text/event-stream"}, stream=BytesStream()
        )

    async def broken_send(message):
        assert message["type"] == "http.response.start"
        raise OSError("uncertain delivery")

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        app.state.inference_client = client
        response = ProxyResponse(app, chat=True)
        await drive(response, broken_send)
    assert response.commit_started and not response.response_started
    assert response.body_bytes_sent == 0
    assert len(calls) == 1
    assert app.state.ledger.snapshot()["active_requests"] == 0


async def test_downstream_send_timeout_cleans_up_without_retry(settings):
    settings = settings.model_copy(update={"downstream_timeout_seconds": 0.02})
    app = prepared(settings)
    calls = []

    async def transport(request):
        calls.append(request)
        return httpx.Response(
            200, headers={"content-type": "text/event-stream"}, stream=BytesStream()
        )

    async def slow_send(message):
        if message["type"] == "http.response.body":
            await asyncio.Event().wait()

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        app.state.inference_client = client
        async with asyncio.timeout(1):
            await drive(ProxyResponse(app, chat=True), slow_send)
    assert len(calls) == 1
    assert app.state.ledger.snapshot()["outcomes"] == {"timeout": 1}
    assert app.state.ledger.snapshot()["active_attempts"] == 0


@pytest.mark.parametrize("failure", ["raise", "timeout"])
async def test_l5_close_failure_releases_local_state_and_quarantines(settings, failure):
    settings = settings.model_copy(update={"cleanup_timeout_seconds": 0.02})
    app = prepared(settings)
    response = ProxyResponse(app, chat=True)
    response.lease = app.state.ledger.acquire(RequestContext("close-test", "fake-model"))
    response.attempt = app.state.ledger.select_and_reserve(response.lease, 9000)
    worker_id = response.attempt.worker_id
    closed = asyncio.Event()

    class BadClose:
        async def aclose(self):
            try:
                if failure == "timeout":
                    await asyncio.Event().wait()
                raise OSError("cannot close")
            finally:
                closed.set()

    response.upstream = BadClose()
    async with asyncio.timeout(1):
        await response._finish()
    assert closed.is_set()
    assert app.state.ledger.snapshot()["active_requests"] == 0
    assert app.state.ledger.snapshot()["active_attempts"] == 0
    worker = next(w for w in app.state.registry.snapshots() if w.worker_id == worker_id)
    assert worker.health is Health.SUSPECT
    assert app.state.telemetry.counts["cleanup_failures"] == 1


async def test_cancel_during_connect_cleans_reservation(settings):
    app = prepared(settings)
    entering = asyncio.Event()

    async def transport(request):
        entering.set()
        await asyncio.Event().wait()

    async def send(message):
        pass

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        app.state.inference_client = client
        task = asyncio.create_task(drive(ProxyResponse(app, chat=True), send))
        await entering.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert app.state.ledger.snapshot()["active_requests"] == 0
    assert app.state.ledger.snapshot()["active_attempts"] == 0
    assert app.state.telemetry.counts["cancelled"] == 1


@pytest.mark.parametrize(
    "body,code",
    [
        (b"data: " + b"x" * 100, "upstream_event_too_large"),
        (b": comment\n\n" * 30, "upstream_prefetch_too_large"),
        (b"data: not-json\n\n", "invalid_upstream_event"),
    ],
)
async def test_invalid_or_unbounded_prelude_is_not_retried(settings, body, code):
    settings = settings.model_copy(update={"max_event_bytes": 64, "max_prefetch_bytes": 64})
    app = prepared(settings)
    calls, sent = [], []

    class ScriptedStream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield body

    async def transport(request):
        calls.append(request)
        return httpx.Response(
            200, headers={"content-type": "text/event-stream"}, stream=ScriptedStream()
        )

    async def send(message):
        sent.append(message)

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        app.state.inference_client = client
        await drive(ProxyResponse(app, chat=True), send)
    assert len(calls) == 1
    assert sent[0]["status"] == 502
    assert code.encode() in sent[-1]["body"]


@pytest.mark.parametrize("status", [400, 429, 500])
async def test_upstream_business_failure_is_not_retried(settings, status):
    app = prepared(settings)
    calls, sent = [], []

    async def transport(request):
        calls.append(request)
        return httpx.Response(status, json={"error": "injected"})

    async def send(message):
        sent.append(message)

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        app.state.inference_client = client
        await drive(ProxyResponse(app, chat=True), send)
    assert len(calls) == 1
    assert sent[0]["status"] == (status if status < 500 else 502)


async def test_delivered_response_stays_successful_when_cleanup_outlives_overall(settings):
    settings = settings.model_copy(
        update={
            "overall_timeout_seconds": 0.03,
            "cleanup_timeout_seconds": 0.2,
        }
    )
    app = prepared(settings)
    sent = []
    closed = asyncio.Event()

    class SlowClose(BytesStream):
        async def aclose(self):
            await asyncio.sleep(0.08)
            closed.set()

    async def transport(request):
        return httpx.Response(
            200, headers={"content-type": "text/event-stream"}, stream=SlowClose()
        )

    async def send(message):
        sent.append(message)

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        app.state.inference_client = client
        await drive(ProxyResponse(app, chat=True), send)
    assert closed.is_set()
    assert sum(m["type"] == "http.response.body" and not m["more_body"] for m in sent) == 1
    assert app.state.ledger.snapshot()["outcomes"] == {"completed": 1}
    assert app.state.telemetry.counts["timeouts"] == 0


def test_old_attempt_quarantine_cannot_poison_new_instance(settings):
    app = prepared(settings)
    registry = app.state.registry
    worker = registry.snapshots()[0]
    new_epoch = registry.replace_instance(worker.worker_id)
    registry.record_health(worker.worker_id, new_epoch, True, registry.clock())
    assert not registry.quarantine(worker.worker_id, worker.epoch)
    assert registry.snapshots()[0].health is Health.HEALTHY
