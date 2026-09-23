"""Server protocol/usage checks; never fabricates GPU performance or cancellation proof."""

import argparse
import asyncio
import json
from pathlib import Path
from time import monotonic

import httpx

from inferscale.sse import SSEParser, StreamInspector


async def verify(gateway, workers, model, expected_policy=None):
    report = {
        "model": model,
        "checks": [],
        "gpu_cancel_verified": False,
        "performance_verified": False,
    }
    async with httpx.AsyncClient(timeout=60, trust_env=False) as client:
        live = await client.get(gateway + "/health/live")
        live.raise_for_status()
        actual_policy = live.json().get("policy")
        if actual_policy not in {"round_robin", "least_load", "cost", "prefix", "prefix_v2"}:
            raise ValueError("Gateway does not report a recognized active policy")
        if expected_policy is not None and actual_policy != expected_policy:
            raise ValueError(f"Policy mismatch: expected {expected_policy}, got {actual_policy}")
        report["gateway_policy"] = actual_policy
        report["expected_policy"] = expected_policy
        ready = await client.get(gateway + "/health/ready")
        ready.raise_for_status()
        assert ready.json()["ready_workers"] == 2, "expected two healthy replicas"
        for chat in (False, True):
            prompt = (
                {"messages": [{"role": "user", "content": "Reply with one short sentence: hello."}]}
                if chat
                else {"prompt": "Write one short sentence about a tree:"}
            )
            tokens = []
            for worker in workers:
                response = await client.post(worker + "/tokenize", json={"model": model, **prompt})
                response.raise_for_status()
                data = response.json()
                assert data["count"] == len(data["tokens"])
                tokens.append(data["tokens"])
            assert tokens[0] == tokens[1], "replicas disagree on tokenizer/template"
            for endpoint in [*workers, gateway]:
                for stream in (False, True):
                    body = {
                        "model": model,
                        **prompt,
                        "stream": stream,
                        "max_tokens": 32,
                        "temperature": 0,
                    }
                    if stream:
                        body["stream_options"] = {"include_usage": True}
                    path = "/v1/chat/completions" if chat else "/v1/completions"
                    started = monotonic()
                    async with client.stream("POST", endpoint + path, json=body) as response:
                        response.raise_for_status()
                        if stream:
                            parser, inspector = SSEParser(65536), StreamInspector(chat)
                            async for chunk in response.aiter_bytes():
                                for event in parser.feed(chunk):
                                    inspector.inspect(event)
                            assert inspector.done, "stream missing valid finish/DONE"
                            usage = inspector.usage
                        else:
                            raw = await response.aread()
                            data = json.loads(raw)
                            assert len(data["choices"]) == 1 and data["choices"][0]["finish_reason"]
                            usage = data.get("usage")
                    assert usage and usage["prompt_tokens"] == len(tokens[0]), (
                        "usage/template mismatch"
                    )
                    assert type(usage["completion_tokens"]) is int
                    assert 0 <= usage["completion_tokens"] <= 32
                    assert (
                        usage["total_tokens"] == usage["prompt_tokens"] + usage["completion_tokens"]
                    )
                    report["checks"].append(
                        {
                            "endpoint": endpoint,
                            "chat": chat,
                            "stream": stream,
                            "seconds": monotonic() - started,
                            "usage": usage,
                        }
                    )
        metrics = await client.get(gateway + "/metrics")
        metrics.raise_for_status()
        report["gateway_metrics"] = metrics.text
    report["protocol_usage_passed"] = True
    return report


async def main(args):
    report = await verify(args.gateway, args.workers, args.model, args.expect_policy)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    await asyncio.to_thread(args.output.write_text, json.dumps(report, indent=2), encoding="utf-8")
    print(f"PASS: 12 generation checks, tokenizer equality and usage. Saved {args.output}")
    print("GPU cancellation and performance remain unverified; see deploy/README.md.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--gateway", default="http://127.0.0.1:8000")
    parser.add_argument(
        "--workers", nargs=2, default=["http://127.0.0.1:8100", "http://127.0.0.1:8101"]
    )
    parser.add_argument("--model", default="inference-model")
    parser.add_argument(
        "--expect-policy", choices=["round_robin", "least_load", "cost", "prefix", "prefix_v2"]
    )
    parser.add_argument("--output", type=Path, default=Path("runs/server-smoke.json"))
    asyncio.run(main(parser.parse_args()))
