"""Real-socket CPU stability check; synthetic workload, not a GPU benchmark."""

import argparse
import asyncio
import json
import time
import tracemalloc
from collections import Counter
from pathlib import Path

import httpx

from inferscale.testing.worker import FakeSettings
from tests.integration.test_fake_workers import payload
from tests.support import cluster, settled


async def run(seconds: float, output: Path):
    tracemalloc.start()
    results = Counter()
    samples = []
    started = time.monotonic()
    async with (
        cluster(
            FakeSettings(worker_id="worker-0", first_delay=0.5, chunk_delay=0.005),
            FakeSettings(worker_id="worker-1", first_delay=0.5, chunk_delay=0.005),
        ) as (app, url, a, b),
        httpx.AsyncClient(trust_env=False, timeout=5) as client,
    ):

        async def one():
            try:
                response = await client.post(f"{url}/v1/chat/completions", json=payload())
                if response.status_code != 200 or "data: [DONE]" not in response.text:
                    raise AssertionError(f"unexpected response: {response.status_code}")
                results["completed"] += 1
            except asyncio.CancelledError:
                results["client_cancelled"] += 1

        batch = 0
        next_sample = 0.0
        while time.monotonic() - started < seconds:
            tasks = [asyncio.create_task(one()) for _ in range(6)]
            try:
                # Wall-clock cancellation can fire before HTTP even reaches the
                # gateway under tracing. Require all six actual upstream streams
                # to be active first, then abort exactly two clients.
                async with asyncio.timeout(5):
                    while a.state.counters["active"] + b.state.counters["active"] != 6:
                        if any(task.done() for task in tasks):
                            raise AssertionError("batch finished before upstream cancel barrier")
                        await asyncio.sleep(0.001)
                tasks[0].cancel()
                tasks[3].cancel()
                await asyncio.gather(*tasks)
            finally:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
            await settled(app, a, b)
            batch += 1
            assert app.state.telemetry.counts["cancelled"] == batch * 2
            assert a.state.counters["cancelled"] + b.state.counters["cancelled"] == batch * 2
            elapsed = time.monotonic() - started
            if elapsed >= next_sample:
                pool = (
                    app.state.inference_client._transport._pool
                )  # Observability only, pinned HTTPX.
                current, peak = tracemalloc.get_traced_memory()
                sample = {
                    "seconds": round(elapsed, 2),
                    "python_bytes": current,
                    "python_peak_bytes": peak,
                    "asyncio_tasks": len(asyncio.all_tasks()),
                    "upstream_connections": len(pool.connections),
                    "active_requests": app.state.ledger.snapshot()["active_requests"],
                }
                samples.append(sample)
                print(json.dumps({**sample, **results}), flush=True)
                next_sample = elapsed + 30
            await asyncio.sleep(0.05)
        await settled(app, a, b)
        ledger = app.state.ledger.snapshot()
        report = {
            "kind": "CPU synthetic real-socket soak, not performance evidence",
            "cancellation": "two clients cancelled after all six upstream streams are active",
            "requested_seconds": seconds,
            "elapsed_seconds": time.monotonic() - started,
            "client_results": dict(results),
            "ledger": ledger,
            "gateway_counters": dict(app.state.telemetry.counts),
            "fake_workers": [dict(a.state.counters), dict(b.state.counters)],
            "samples": samples,
        }
        assert ledger["active_requests"] == ledger["active_attempts"] == 0
        assert app.state.telemetry.counts["cleanup_failures"] == 0
        assert app.state.telemetry.counts["failed"] == 0
        assert app.state.telemetry.counts["timeouts"] == 0
        assert app.state.telemetry.counts["completed"] == results["completed"]
        assert app.state.telemetry.counts["cancelled"] == results["client_cancelled"] > 0
        assert all(s["upstream_connections"] <= app.state.settings.global_capacity for s in samples)
        # Exclude startup; look for large sustained growth, not allocator noise.
        stable = [s for s in samples if s["seconds"] >= 60]
        if len(stable) > 2:
            assert stable[-1]["python_bytes"] - stable[0]["python_bytes"] < 8_000_000
            assert (
                max(s["asyncio_tasks"] for s in stable) - min(s["asyncio_tasks"] for s in stable)
                <= 8
            )
        output.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(output.write_text, json.dumps(report, indent=2), encoding="utf-8")
        print(f"PASS: report saved to {output}", flush=True)
    tracemalloc.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=float, default=1200)
    parser.add_argument("--output", type=Path, default=Path("runs/p2-soak.json"))
    args = parser.parse_args()
    if args.seconds <= 0:
        parser.error("seconds must be positive")
    asyncio.run(run(args.seconds, args.output))
