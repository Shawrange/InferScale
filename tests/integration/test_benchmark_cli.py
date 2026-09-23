import json
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
import yaml

from inferscale.benchmark.__main__ import run
from inferscale.benchmark.compare import compare
from inferscale.benchmark.report import write_report
from inferscale.benchmark.workloads import Item, Trace
from tests.support import cluster, settled


async def test_complete_cli_artifacts_and_recomputable_report():
    base = Path("runs") / ("p4-cli-test-" + uuid4().hex)
    base.mkdir(parents=True)
    async with cluster(routing={"policy": "cost"}) as (app, url, a, b):
        log = base / "gateway.log"

        def record(**fields):
            with log.open("a", encoding="utf-8") as file:
                file.write(json.dumps(fields) + "\n")

        app.state.telemetry.log = record
        config = base / "server.yaml"
        config.write_text(
            yaml.safe_dump(app.state.settings.model_dump(mode="json")), encoding="utf-8"
        )
        trace = Trace(
            workload="mixed",
            seed=42,
            duration=0.3,
            rate=10,
            warmup=(),
            requests=(
                Item(offset=0, prompt="hello", max_tokens=16, group="short"),
                Item(offset=0.1, prompt="hello world", max_tokens=16, group="short"),
            ),
        )
        trace_path = base / "trace.json"
        trace_path.write_text(trace.model_dump_json(), encoding="utf-8")
        args = SimpleNamespace(
            trace=trace_path,
            config=config,
            gateway=url,
            policy="cost",
            output=base / "result",
            mode="open_loop",
            capacity=4,
            timeout=2,
            drain=1,
            lag_tolerance=0.1,
            round=1,
            gateway_log=log,
            environment=None,
            cache_condition="fresh_seeded",
            ttft_slo=1,
            e2e_slo=2,
        )
        await run(args)
        await settled(app, a, b)
        summary = json.loads((args.output / "summary.json").read_text(encoding="utf-8"))
        assert summary["attempt_evidence"]["complete"]
        assert summary["outcomes"]["succeeded"] == 2 and summary["attempt_count"] == 2
        assert "synthetic_backend" in summary["comparison_exclusion_reasons"]
        assert summary == write_report(args.output, log_text=log.read_text(encoding="utf-8"))
        assert len((args.output / "attempts.jsonl").read_text().splitlines()) == 2
        assert (args.output / "before-worker0.prom").exists()
        compare([args.output], base / "comparison")
        result = json.loads((base / "comparison/comparison.json").read_text())
        assert result[0]["usable_rounds"] == 0
        raw_path = args.output / "requests.raw.jsonl"
        raw_path.write_text(
            raw_path.read_text(encoding="utf-8").splitlines()[0] + "\n", encoding="utf-8"
        )
        with pytest.raises(ValueError):
            write_report(args.output, log_text=log.read_text(encoding="utf-8"))
        args.timeout = float("nan")
        with pytest.raises(ValueError, match="invalid client limits"):
            await run(args)
