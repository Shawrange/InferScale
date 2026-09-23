import asyncio
import hashlib
import json
from collections import Counter
from time import monotonic
from uuid import uuid4

import httpx

from inferscale.api import APIError
from inferscale.samples import RequestSample
from inferscale.sse import SSEParser, StreamInspector


async def wait_until(deadline):
    # Some event loops wake timers early. Recheck the absolute clock instead of
    # sending before the trace time or silently changing the scheduled timestamp.
    while (remaining := deadline - monotonic()) > 0:  # noqa: ASYNC110 -- absolute timer, not an event
        await asyncio.sleep(max(remaining, 0.001))


def body_for(item, model):
    return {
        "model": model,
        "messages": [{"role": "user", "content": item.prompt}],
        "stream": True,
        "stream_options": {"include_usage": True},
        "max_tokens": item.max_tokens,
        "temperature": 0,
        "top_p": 1,
    }


async def request_one(client, url, model, item, scheduled, origin, budget_seconds, request_id):
    sent = monotonic() - origin
    fields = dict(request_id=request_id, scheduled_at=scheduled, sent_at=sent, group=item.group)
    first = last = None
    intervals = []
    outcome, error = "failed", None
    usage, finish = None, None
    try:
        async with asyncio.timeout(budget_seconds):
            async with client.stream(
                "POST",
                url + "/v1/chat/completions",
                headers={"x-inferscale-client-id": request_id},
                json=body_for(item, model),
            ) as response:
                fields["gateway_request_id"] = response.headers.get("x-inferscale-request-id")
                if response.status_code != 200:
                    raw = bytearray()
                    async for chunk in response.aiter_bytes():
                        if len(raw) + len(chunk) > 65536:
                            break
                        raw.extend(chunk)
                    code = None
                    try:
                        code = json.loads(raw).get("error", {}).get("code")
                    except (ValueError, AttributeError):
                        pass
                    admission = code in {
                        "global_capacity_exceeded",
                        "no_worker_capacity",
                        "tokenizer_capacity_exceeded",
                        "upstream_pool_timeout",
                    }
                    outcome = (
                        "rejected"
                        if response.status_code in {400, 401, 403, 413, 422, 429} or admission
                        else "failed"
                    )
                    error = str(code or f"http_{response.status_code}")
                else:
                    if "text/event-stream" not in response.headers.get("content-type", ""):
                        raise APIError(502, "invalid_content_type")
                    parser, inspector = SSEParser(65536), StreamInspector(True)
                    async for chunk in response.aiter_bytes():
                        for event in parser.feed(chunk):
                            if inspector.inspect(event):
                                now = monotonic() - origin
                                if last is not None:
                                    intervals.append(now - last)
                                first = now if first is None else first
                                last = now
                            if inspector.done:
                                break
                        if inspector.done:
                            break
                    if inspector.done:
                        outcome = "succeeded"
                        usage, finish = inspector.usage, inspector.finish_reason
                    else:
                        error = "missing_valid_done"
    except asyncio.CancelledError:
        outcome, error = "cancelled", "drain_deadline"
    except (TimeoutError, httpx.TimeoutException):
        outcome, error = "failed", "timeout"
    except APIError as exc:
        error = exc.code
    except httpx.HTTPError:
        error = "transport_error"
    terminal = monotonic() - origin
    valid_usage = (
        isinstance(usage, dict)
        and all(
            type(usage.get(k)) is int
            for k in ("prompt_tokens", "completion_tokens", "total_tokens")
        )
        and usage["prompt_tokens"] >= 0
        and 0 <= usage["completion_tokens"] <= item.max_tokens
        and usage["total_tokens"] == usage["prompt_tokens"] + usage["completion_tokens"]
    )
    return RequestSample(
        **fields,
        first_content_at=first,
        last_content_at=last,
        terminal_at=terminal,
        outcome=outcome,
        error_subtype=error,
        prompt_tokens=usage["prompt_tokens"] if valid_usage else None,
        output_tokens=usage["completion_tokens"] if valid_usage else None,
        finish_reason=finish,
        chunk_intervals=tuple(intervals),
    )


async def run_trace(
    trace,
    gateway,
    model,
    policy,
    *,
    mode="open_loop",
    capacity=32,
    budget_seconds=60,
    drain=65,
    lag_tolerance=0.05,
    workers=(),
    headers=None,
    on_sample=None,
):
    if mode not in {"open_loop", "closed_loop"} or capacity <= 0 or min(budget_seconds, drain) <= 0:
        raise ValueError("invalid runner limits")
    rows = [None] * len(trace.requests)
    active = set()
    background = set()
    metric_errors = []
    snapshots = {}
    async with (
        httpx.AsyncClient(
            timeout=None,
            trust_env=False,
            headers=headers,
            limits=httpx.Limits(max_connections=capacity),
        ) as client,
        httpx.AsyncClient(timeout=5, trust_env=False) as observer,
    ):
        live = await client.get(gateway + "/health/live", timeout=5)
        live.raise_for_status()
        if live.json().get("policy") != policy:
            raise ValueError("Gateway policy mismatch; no traffic sent")
        ready = await client.get(gateway + "/health/ready", timeout=5)
        ready.raise_for_status()
        if ready.json().get("ready_workers") != len(workers) and workers:
            raise ValueError("unexpected ready worker count")

        async def wait_idle():
            async with asyncio.timeout(5):
                while True:
                    response = await client.get(gateway + "/metrics", timeout=5)
                    response.raise_for_status()
                    values = {
                        line.split()[0]: float(line.split()[1])
                        for line in response.text.splitlines()
                        if line.startswith(
                            ("inferscale_active_requests ", "inferscale_active_attempts ")
                        )
                    }
                    if (
                        values.get("inferscale_active_requests")
                        == values.get("inferscale_active_attempts")
                        == 0
                    ):
                        return
                    await asyncio.sleep(0.01)

        await wait_idle()

        async def capture(label):
            for index, endpoint in enumerate(workers):
                try:
                    response = await observer.get(endpoint + "/metrics")
                    response.raise_for_status()
                    snapshots[f"{label}-worker{index}.prom"] = response.text
                except httpx.HTTPError as exc:
                    metric_errors.append(f"{label}:worker{index}:{type(exc).__name__}")

        # Check actual template lengths for every unique prompt before measurement.
        # Preflight is rendering only, with no fabricated token estimates.
        counts = {}
        if workers:
            for item in (*trace.warmup, *trace.requests):
                if item.prompt not in counts:
                    reply = await observer.post(
                        workers[0] + "/tokenize",
                        json={
                            "model": model,
                            "messages": [{"role": "user", "content": item.prompt}],
                        },
                    )
                    reply.raise_for_status()
                    data = reply.json()
                    if type(data.get("count")) is not int or data["count"] != len(data["tokens"]):
                        raise ValueError("invalid preflight tokenizer response")
                    counts[item.prompt] = (data["count"], data["max_model_len"])
                count, limit = counts[item.prompt]
                if count + item.max_tokens > limit:
                    raise ValueError("trace exceeds backend context; regenerate before running")
        warmup_rows = []
        for item in trace.warmup:
            warm = await request_one(
                client, gateway, model, item, 0, monotonic(), budget_seconds, uuid4().hex
            )
            warmup_rows.append(warm.model_dump())
            if warm.outcome != "succeeded":
                raise ValueError(f"warmup failed: {warm.error_subtype}")
        await wait_idle()
        await capture("before")
        origin = monotonic()

        async def measured(index, scheduled, request_id):
            rows[index] = await request_one(
                client,
                gateway,
                model,
                trace.requests[index],
                scheduled,
                origin,
                budget_seconds,
                request_id,
            )
            rows[index] = rows[index].model_copy(update={"trace_index": index})
            if on_sample:
                on_sample(rows[index])

        async def end_snapshot():
            await wait_until(origin + trace.duration)
            await capture("window-end")

        snapshot_task = asyncio.create_task(end_snapshot())
        background.add(snapshot_task)
        try:
            for index, item in enumerate(trace.requests):
                scheduled = item.offset if mode == "open_loop" else monotonic() - origin
                if mode == "open_loop":
                    await wait_until(origin + scheduled)
                elif len(active) >= capacity:
                    _, pending = await asyncio.wait(
                        active,
                        timeout=max(0, origin + trace.duration - monotonic()),
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    active.intersection_update(pending)
                    scheduled = monotonic() - origin
                now = monotonic() - origin
                request_id = uuid4().hex
                if now >= trace.duration or len(active) >= capacity:
                    rows[index] = RequestSample(
                        request_id=request_id,
                        scheduled_at=scheduled,
                        terminal_at=max(now, scheduled),
                        outcome="not_sent",
                        attempt_count=0,
                        group=item.group,
                        trace_index=index,
                        error_subtype="window_closed"
                        if now >= trace.duration
                        else "client_capacity",
                    )
                    if on_sample:
                        on_sample(rows[index])
                    continue
                task = asyncio.create_task(measured(index, scheduled, request_id))
                active.add(task)
                task.add_done_callback(active.discard)
                # Keep references for all task exceptions, without retaining unbounded live work.
                background.add(task)
            await wait_until(origin + trace.duration)
            if active:
                _, pending = await asyncio.wait(
                    active, timeout=max(0, origin + trace.duration + drain - monotonic())
                )
                for task in pending:
                    task.cancel()
            await asyncio.gather(*background)
        finally:
            for task in background:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*background, return_exceptions=True)
        await capture("after-drain")
    assert all(row is not None for row in rows)
    lags = [r.sent_at - r.scheduled_at for r in rows if r.sent_at is not None]
    overloaded = any(r.outcome == "not_sent" for r in rows) or any(
        lag > lag_tolerance for lag in lags
    )
    preflight = [
        {
            "prompt_sha256": hashlib.sha256(p.encode()).hexdigest(),
            "prompt_tokens": c[0],
            "backend_context": c[1],
        }
        for p, c in counts.items()
    ]
    return (
        rows,
        {
            "mode": mode,
            "duration": trace.duration,
            "capacity": capacity,
            "client_timeout": budget_seconds,
            "drain_seconds": drain,
            "lag_tolerance": lag_tolerance,
            "load_generator_limited": overloaded,
            "scheduled_to_sent_max": max(lags, default=None),
            "outcomes": dict(Counter(r.outcome for r in rows)),
            "policy": policy,
            "preflight": preflight,
            "warmup": warmup_rows,
            "metric_errors": metric_errors,
        },
        snapshots,
    )
