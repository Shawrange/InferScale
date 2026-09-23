import asyncio
import json

from fastapi import FastAPI
from fastapi.responses import StreamingResponse

from inferscale.config import Settings, load_settings
from inferscale.main import create_app
from inferscale.testing.worker import sse
from tests.integration.test_fake_workers import payload, serve
from tests.support import settled


async def test_real_socket_slow_reader_times_out_and_releases_upstream():
    worker = FastAPI()
    closed = asyncio.Event()

    @worker.get("/health")
    async def health():
        return {"ok": True}

    @worker.post("/v1/chat/completions")
    async def generate():
        async def content():
            try:
                # Enough to exhaust TCP/Uvicorn buffers; do not materialize the full stream.
                for _ in range(4096):
                    yield sse(
                        {
                            "id": "flood",
                            "choices": [
                                {
                                    "index": 0,
                                    "delta": {"content": "x" * 16_384},
                                    "finish_reason": None,
                                }
                            ],
                        }
                    )
            finally:
                closed.set()

        return StreamingResponse(content(), media_type="text/event-stream")

    async with serve(worker) as upstream:
        values = load_settings("configs/local.yaml").model_dump(mode="json")
        values["workers"] = [values["workers"][0] | {"endpoint": upstream}]
        values.update(downstream_timeout_seconds=0.1, overall_timeout_seconds=10)
        app = create_app(Settings.model_validate(values))
        async with serve(app) as url:
            port = int(url.rsplit(":", 1)[1])
            reader, writer = await asyncio.open_connection("127.0.0.1", port, limit=4096)
            try:
                body = json.dumps(payload()).encode()
                writer.write(
                    b"POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\n"
                    b"Content-Type: application/json\r\nContent-Length: "
                    + str(len(body)).encode()
                    + b"\r\n\r\n"
                    + body
                )
                await writer.drain()
                headers = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 2)
                assert b"200 OK" in headers
                # No response body reads. StreamReader and TCP receive windows eventually fill.
                await asyncio.wait_for(closed.wait(), 5)
                await settled(app)
                assert app.state.telemetry.counts["timeouts"] == 1
                assert app.state.telemetry.counts["retries"] == 0
                assert app.state.telemetry.counts["partial_failures"] == 1
            finally:
                writer.close()
                await writer.wait_closed()


async def test_slow_inbound_body_has_deadline_and_no_attempt():
    values = load_settings("configs/local.yaml").model_dump(mode="json")
    values.update(body_timeout_seconds=0.05)
    app = create_app(Settings.model_validate(values))
    async with serve(app) as url:
        port = int(url.rsplit(":", 1)[1])
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        try:
            writer.write(
                b"POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\n"
                b"Content-Length: 100\r\n\r\n{"
            )
            await writer.drain()
            headers = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 2)
            assert b"504 Gateway Timeout" in headers
            await settled(app)
            assert app.state.ledger.snapshot()["active_attempts"] == 0
        finally:
            writer.close()
            await writer.wait_closed()
