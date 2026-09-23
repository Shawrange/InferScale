import argparse
import asyncio
import json
from collections import deque
from collections.abc import AsyncIterator
from typing import Literal
from uuid import uuid4

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

from inferscale.features import fixture_token_count, fixture_token_ids


class FakeSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    worker_id: str = "worker-0"
    model: str = "fake-model"
    scenario: Literal["success", "before-content", "after-content", "http-error", "empty"] = (
        "success"
    )
    first_delay: float = Field(default=0, ge=0, le=60)
    chunk_delay: float = Field(default=0, ge=0, le=60)
    error_status: Literal[502, 503, 504] = 503


class Message(BaseModel):
    model_config = ConfigDict(extra="forbid")
    role: Literal["system", "user", "assistant"]
    content: str


class GenerationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    model: str
    stream: bool = False
    max_tokens: int = Field(default=16, gt=0, le=4096)
    n: Literal[1] = 1
    temperature: float | None = None
    top_p: float | None = None
    stream_options: dict | None = None


class ChatRequest(GenerationRequest):
    messages: list[Message] = Field(min_length=1)


class CompletionRequest(GenerationRequest):
    prompt: str


def sse(data: dict | str) -> bytes:
    payload = data if isinstance(data, str) else json.dumps(data, ensure_ascii=False)
    return f"data: {payload}\n\n".encode()


def create_fake_app(settings: FakeSettings) -> FastAPI:
    app = FastAPI(title=f"Fake vLLM ({settings.worker_id})")
    state = {"total": 0, "active": 0, "completed": 0, "truncated": 0, "cancelled": 0}
    events: deque[dict] = deque(maxlen=256)
    app.state.counters = state
    app.state.idle = asyncio.Event()
    app.state.idle.set()

    def begin_execution():
        state["active"] += 1
        app.state.idle.clear()

    def end_execution(event):
        if event["outcome"] == "started":
            state["cancelled"] += 1
            event["outcome"] = "cancelled"
        state["active"] -= 1
        if state["active"] == 0:
            app.state.idle.set()

    @app.get("/health")
    async def health():
        return {"status": "ok", "worker_id": settings.worker_id, "fake": True}

    @app.get("/metrics", response_class=PlainTextResponse)
    async def metrics():
        return (
            "# TYPE vllm:num_requests_running gauge\n"
            f"vllm:num_requests_running {state['active']}\n"
            "# TYPE vllm:num_requests_waiting gauge\n"
            "vllm:num_requests_waiting 0\n"
            "# TYPE fake_requests_total counter\n"
            f"fake_requests_total {state['total']}\n"
        )

    @app.get("/_test/requests")
    async def history():
        # Test server only; bounded history with IDs/outcomes, never prompts.
        return {"worker_id": settings.worker_id, "counters": dict(state), "events": list(events)}

    @app.post("/tokenize")
    async def tokenize(request: Request):
        body = await request.json()
        if body.get("model") != settings.model:
            return JSONResponse({"error": "unknown_model"}, status_code=400)
        ids = fixture_token_ids(body, "messages" in body)
        return {"count": len(ids), "tokens": ids, "max_model_len": 4096}

    async def generate(body: GenerationRequest, request: Request, chat: bool):
        if body.model != settings.model:
            return JSONResponse({"error": {"code": "unknown_model"}}, status_code=400)
        state["total"] += 1
        response_id = f"{settings.worker_id}-{uuid4().hex}"
        event = {
            "request_id": request.headers.get("x-request-id", response_id),
            "attempt_id": request.headers.get("x-attempt-id", response_id),
            "response_id": response_id,
            "outcome": "started",
        }
        events.append(event)
        if settings.scenario == "http-error":
            event["outcome"] = "http_error"
            return JSONResponse({"error": {"code": "injected_error"}}, settings.error_status)

        pieces = [] if settings.scenario == "empty" else ["Hello", " ", "世界"][: body.max_tokens]
        # Synthetic fixture units, NOT a tokenizer or real GPU measurements.
        prompt_tokens = fixture_token_count(body.model_dump(), chat)
        usage = {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": len(pieces),
            "total_tokens": prompt_tokens + len(pieces),
        }

        def chunk(choice: dict, usage_value=None):
            data = {
                "id": response_id,
                "object": "chat.completion.chunk" if chat else "text_completion",
                "created": 0,
                "model": body.model,
                "choices": [dict(index=0, **choice)],
            }
            if usage_value is not None:
                data["usage"] = usage_value
            return data

        if not body.stream:
            begin_execution()
            try:
                await asyncio.sleep(settings.first_delay)
                if settings.scenario in {"before-content", "after-content"}:
                    event["outcome"] = "http_error"
                    return JSONResponse({"error": {"code": "stream_scenario_requires_stream"}}, 400)
                content = "".join(pieces)
                choice = (
                    {"message": {"role": "assistant", "content": content}}
                    if chat
                    else {"text": content}
                )
                data = chunk(dict(**choice, finish_reason="stop"), usage)
                data["object"] = "chat.completion" if chat else "text_completion"
                state["completed"] += 1
                event["outcome"] = "completed"
                return JSONResponse(data)
            finally:
                end_execution(event)

        async def stream() -> AsyncIterator[bytes]:
            begin_execution()
            try:
                role = {"delta": {"role": "assistant"}} if chat else {"text": ""}
                yield sse(chunk(dict(**role, finish_reason=None)))
                await asyncio.sleep(settings.first_delay)
                if settings.scenario == "before-content":
                    state["truncated"] += 1
                    event["outcome"] = "truncated"
                    return  # Valid HTTP EOF but invalid/incomplete generation stream.
                for piece in pieces:
                    content = {"delta": {"content": piece}} if chat else {"text": piece}
                    yield sse(chunk(dict(**content, finish_reason=None)))
                    await asyncio.sleep(settings.chunk_delay)
                    if settings.scenario == "after-content":
                        state["truncated"] += 1
                        event["outcome"] = "truncated"
                        return
                final = {"delta": {}} if chat else {"text": ""}
                yield sse(chunk(dict(**final, finish_reason="stop"), usage))
                state["completed"] += 1
                event["outcome"] = "completed"
                yield sse("[DONE]")
            finally:
                end_execution(event)

        return StreamingResponse(stream(), media_type="text/event-stream")

    @app.post("/v1/chat/completions")
    async def chat(body: ChatRequest, request: Request):
        return await generate(body, request, True)

    @app.post("/v1/completions")
    async def completion(body: CompletionRequest, request: Request):
        return await generate(body, request, False)

    return app


def main():
    parser = argparse.ArgumentParser(description="CPU-only fake vLLM; metrics are synthetic")
    parser.add_argument("--worker-id", default="worker-0")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8100)
    parser.add_argument(
        "--scenario",
        default="success",
        choices=["success", "before-content", "after-content", "http-error", "empty"],
    )
    parser.add_argument("--first-delay", type=float, default=0)
    parser.add_argument("--chunk-delay", type=float, default=0)
    parser.add_argument("--error-status", type=int, default=503, choices=[502, 503, 504])
    args = parser.parse_args()
    settings = FakeSettings(
        **{key: value for key, value in vars(args).items() if key not in {"host", "port"}}
    )
    uvicorn.run(create_fake_app(settings), host=args.host, port=args.port, workers=1)


if __name__ == "__main__":
    main()
