import asyncio

import httpx
import pytest

from inferscale.api import APIError
from inferscale.tokenization import Tokenizer


@pytest.mark.parametrize("chat", [True, False])
async def test_remote_tokenizer_payload_and_ids(pool, settings, chat):
    registry, _, _ = pool
    tokenizer = Tokenizer(settings.model_copy(update={"tokenizer_mode": "vllm"}), registry)
    body = {
        "model": "fake-model",
        "prompt": "hello",
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 10,
    }

    async def handler(request):
        import json

        data = json.loads(request.content)
        assert set(data) == {"model", "messages" if chat else "prompt"}
        assert request.url.path == "/tokenize"
        return httpx.Response(200, json={"count": 3, "tokens": [1, 5, 2]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        tokenizer.client = client
        assert await tokenizer.encode(body, chat) == (1, 5, 2)


@pytest.mark.parametrize(
    "result", [{"count": 2, "tokens": [1]}, {"count": 1, "tokens": [True]}, {}]
)
async def test_bad_remote_tokens_are_not_estimated(pool, settings, result):
    registry, _, _ = pool
    tokenizer = Tokenizer(settings.model_copy(update={"tokenizer_mode": "vllm"}), registry)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json=result))
    ) as client:
        tokenizer.client = client
        with pytest.raises(APIError, match="invalid_tokenizer_response"):
            await tokenizer.encode({"model": "fake-model", "prompt": "hi"}, False)


async def test_remote_slots_fast_reject_and_cancel_releases(pool, settings):
    registry, _, _ = pool
    tokenizer = Tokenizer(
        settings.model_copy(update={"tokenizer_mode": "vllm", "tokenizer_slots": 1}), registry
    )
    entered = asyncio.Event()

    async def handler(request):
        entered.set()
        await asyncio.Event().wait()

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        tokenizer.client = client
        body = {"model": "fake-model", "prompt": "hi"}
        task = asyncio.create_task(tokenizer.encode(body, False))
        await entered.wait()
        with pytest.raises(APIError, match="tokenizer_capacity_exceeded"):
            await tokenizer.encode(body, False)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not tokenizer.slots.locked()
