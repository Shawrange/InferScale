import argparse
import asyncio
import os
from contextlib import asynccontextmanager, suppress

import httpx
import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse, PlainTextResponse

from inferscale.config import Settings, load_settings
from inferscale.features import OutputPredictor, TokenCounter
from inferscale.ledger import RequestLedger
from inferscale.proxy import ProxyResponse
from inferscale.registry import WorkerRegistry
from inferscale.telemetry import Telemetry, configure_logging
from inferscale.tokenization import Tokenizer


async def probe_once(registry: WorkerRegistry, client: httpx.AsyncClient):
    async def probe(worker):
        observed_at = registry.clock()
        try:
            response = await client.get(f"{worker.endpoint}/health")
            healthy = response.status_code == 200
        except httpx.HTTPError:
            healthy = False
        registry.record_health(worker.worker_id, worker.epoch, healthy, observed_at)

    await asyncio.gather(*(probe(worker) for worker in registry.snapshots()))


def create_app(settings: Settings, token_counter: TokenCounter | None = None) -> FastAPI:
    if token_counter is None and settings.tokenizer_mode == "fixture":
        if settings.model.tokenizer_identity != "fixture-v1" or settings.model.name != "fake-model":
            raise ValueError("a real model requires an explicitly supplied matching token counter")
    api_key = os.environ.get(settings.api_key_env) if settings.api_key_env else None
    if settings.api_key_env and not api_key:
        raise ValueError("configured API key environment variable is empty")
    registry = WorkerRegistry(settings.workers, settings.health_ttl_seconds)
    ledger = RequestLedger(registry, settings.global_capacity, settings.routing)

    @asynccontextmanager
    async def lifespan(app):
        async with (
            httpx.AsyncClient(
                timeout=settings.health_timeout_seconds,
                trust_env=False,
                limits=httpx.Limits(max_connections=len(settings.workers)),
            ) as client,
            httpx.AsyncClient(
                timeout=httpx.Timeout(
                    connect=settings.connect_timeout_seconds,
                    read=None,
                    write=settings.connect_timeout_seconds,
                    pool=settings.pool_timeout_seconds,
                ),
                limits=httpx.Limits(
                    max_connections=settings.global_capacity,
                    max_keepalive_connections=settings.global_capacity,
                ),
                transport=httpx.AsyncHTTPTransport(
                    retries=0,
                    limits=httpx.Limits(
                        max_connections=settings.global_capacity,
                        max_keepalive_connections=settings.global_capacity,
                    ),
                ),
                trust_env=False,
            ) as inference_client,
            httpx.AsyncClient(
                timeout=settings.tokenizer_timeout_seconds,
                trust_env=False,
                limits=httpx.Limits(max_connections=settings.tokenizer_slots),
            ) as tokenizer_client,
        ):
            app.state.inference_client = inference_client
            app.state.tokenizer.client = tokenizer_client
            await probe_once(registry, client)

            async def collect():
                while True:
                    await asyncio.sleep(settings.health_interval_seconds)
                    await probe_once(registry, client)

            task = asyncio.create_task(collect(), name="worker-health")
            try:
                yield
            finally:
                for worker in registry.snapshots():
                    registry.set_draining(worker.worker_id)
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task

    app = FastAPI(title="InferScale", lifespan=lifespan)
    app.state.registry = registry
    app.state.ledger = ledger
    app.state.settings = settings
    app.state.telemetry = Telemetry()
    app.state.api_key = api_key
    app.state.token_counter = token_counter
    app.state.tokenizer = Tokenizer(settings, registry)
    app.state.predictor = OutputPredictor(settings.model, settings.routing)

    @app.get("/health/live")
    async def live():
        return {"status": "alive", "stage": "P3", "policy": settings.routing.policy}

    @app.get("/health/ready")
    async def ready():
        eligible = registry.candidates(settings.model.name)
        return JSONResponse(
            {"status": "ready" if eligible else "not_ready", "ready_workers": len(eligible)},
            status_code=200 if eligible else 503,
        )

    @app.get("/metrics", response_class=PlainTextResponse)
    async def metrics():
        snapshot = ledger.snapshot()
        return (
            "# TYPE inferscale_active_requests gauge\n"
            f"inferscale_active_requests {snapshot['active_requests']}\n"
            "# TYPE inferscale_active_attempts gauge\n"
            f"inferscale_active_attempts {snapshot['active_attempts']}\n"
            "# TYPE inferscale_ready_workers gauge\n"
            f"inferscale_ready_workers {len(registry.candidates(settings.model.name))}\n"
            + app.state.telemetry.metrics()
        )

    @app.post("/v1/chat/completions")
    async def chat():
        return ProxyResponse(app, chat=True)

    @app.post("/v1/completions")
    async def completion():
        return ProxyResponse(app, chat=False)

    return app


def main():
    parser = argparse.ArgumentParser(description="InferScale single-process generation gateway")
    parser.add_argument("--config", default="configs/local.yaml")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    configure_logging()
    uvicorn.run(create_app(load_settings(args.config)), host=args.host, port=args.port, workers=1)


if __name__ == "__main__":
    main()
