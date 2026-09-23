import asyncio
import json

import httpx

from inferscale.api import APIError
from inferscale.features import fixture_token_ids


class Tokenizer:
    """No queue; remote render requests have separate bounded HTTP slots.

    vLLM mode uses its own template/tokenizer and adds one render round trip.
    This overhead must remain identical in every policy benchmark.
    """

    def __init__(self, settings, registry):
        self.settings, self.registry = settings, registry
        self.slots = asyncio.Semaphore(settings.tokenizer_slots)
        self.client = None

    async def encode(self, body, chat):
        if self.settings.tokenizer_mode == "fixture":
            return fixture_token_ids(body, chat)
        if self.slots.locked():
            raise APIError(503, "tokenizer_capacity_exceeded")
        async with self.slots:
            workers = self.registry.candidates(self.settings.model.name)
            if not workers:
                raise APIError(503, "no_worker_capacity")
            # Deliberately no hidden tokenizer retry; generation retry is separate.
            request = {"model": body["model"]}
            request.update({"messages": body["messages"]} if chat else {"prompt": body["prompt"]})
            try:
                async with asyncio.timeout(self.settings.tokenizer_timeout_seconds):
                    async with self.client.stream(
                        "POST",
                        workers[0].endpoint + "/tokenize",
                        json=request,
                        headers={"accept-encoding": "identity"},
                    ) as response:
                        if response.status_code != 200:
                            raise APIError(502, "tokenizer_rejected")
                        data = bytearray()
                        async for chunk in response.aiter_bytes():
                            if len(data) + len(chunk) > self.settings.max_response_bytes:
                                raise APIError(502, "tokenizer_response_too_large")
                            data.extend(chunk)
                result = json.loads(data)
                ids = result["tokens"]
                if (
                    not isinstance(ids, list)
                    or type(result["count"]) is not int
                    or result["count"] != len(ids)
                    or any(type(i) is not int or i < 0 for i in ids)
                ):
                    raise ValueError
                return tuple(ids)
            except (ValueError, KeyError, TypeError) as exc:
                raise APIError(502, "invalid_tokenizer_response") from exc
            except httpx.TimeoutException:
                raise
            except httpx.HTTPError as exc:
                raise APIError(502, "tokenizer_unavailable") from exc
