import argparse
import asyncio
import hashlib
import importlib.metadata
import json
import math
import os
import platform
import random
from pathlib import Path
from uuid import uuid4

import yaml

from inferscale.benchmark.compare import compare
from inferscale.benchmark.report import write_report
from inferscale.benchmark.runner import run_trace
from inferscale.benchmark.workloads import Trace, generate
from inferscale.config import load_settings

SOURCE_ROOT = Path(__file__).resolve().parents[1]

POLICIES = ["round_robin", "least_load", "cost", "prefix"]


def digest(data):
    return hashlib.sha256(data).hexdigest()


def dump(path, data):
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


async def run(args):
    limits = (args.timeout, args.drain, args.lag_tolerance, args.ttft_slo, args.e2e_slo)
    if (
        not all(math.isfinite(value) for value in limits)
        or args.capacity <= 0
        or min(args.timeout, args.drain, args.ttft_slo, args.e2e_slo) <= 0
        or args.lag_tolerance < 0
        or args.round < 1
    ):
        raise ValueError("invalid client limits")
    trace_bytes = args.trace.read_bytes()
    trace = Trace.model_validate_json(trace_bytes)
    settings = load_settings(args.config)
    if settings.routing.policy != args.policy:
        raise ValueError("config policy does not match --policy")
    if any(i.max_tokens > settings.max_output_tokens for i in (*trace.warmup, *trace.requests)):
        raise ValueError("trace output caps exceed gateway limit")
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output / "trace.json").write_bytes(trace_bytes)
    source = SOURCE_ROOT
    hashes = {
        p.relative_to(source).as_posix(): digest(p.read_bytes())
        for p in sorted(source.rglob("*.py"))
    }
    environment = (
        json.loads(args.environment.read_text(encoding="utf-8")) if args.environment else None
    )
    if environment is not None and (not isinstance(environment, dict) or not environment):
        raise ValueError("environment must be a nonempty JSON object")
    if environment is not None and "REPLACE_" in json.dumps(environment):
        raise ValueError("replace environment template placeholders before running")
    manifest = {
        "schema_version": 1,
        "run_id": uuid4().hex,
        "status": "running",
        "round": args.round,
        "workload": trace.workload,
        "planned_count": len(trace.requests),
        "trace_sha256": digest(trace_bytes),
        "config": settings.model_dump(mode="json"),
        "policy": args.policy,
        "source_hashes": hashes,
        "environment": environment,
        "python": platform.python_version(),
        "dependencies": {
            name: importlib.metadata.version(name) for name in ["httpx", "pydantic", "fastapi"]
        },
        "synthetic": settings.tokenizer_mode == "fixture" or settings.model.name == "fake-model",
        "cache_condition": args.cache_condition,
        "slo": {"ttft": args.ttft_slo, "e2e": args.e2e_slo},
    }
    dump(args.output / "manifest.json", manifest)
    key = os.environ.get(settings.api_key_env) if settings.api_key_env else None
    headers = {"Authorization": f"Bearer {key}"} if key else None
    try:
        with (args.output / "requests.raw.jsonl").open("w", encoding="utf-8") as raw:

            def record(row):
                raw.write(row.model_dump_json() + "\n")
                raw.flush()

            rows, details, snapshots = await run_trace(
                trace,
                args.gateway.rstrip("/"),
                settings.model.name,
                args.policy,
                mode=args.mode,
                capacity=args.capacity,
                budget_seconds=args.timeout,
                drain=args.drain,
                lag_tolerance=args.lag_tolerance,
                workers=[str(w.endpoint).rstrip("/") for w in settings.workers],
                headers=headers,
                on_sample=record,
            )
        manifest.update(details, status="completed")
        (args.output / "requests.jsonl").write_text(
            "".join(r.model_dump_json() + "\n" for r in rows), encoding="utf-8"
        )
        for name, text in snapshots.items():
            (args.output / name).write_text(text, encoding="utf-8")
        dump(args.output / "manifest.json", manifest)
        log_text = None
        if args.gateway_log:
            # Gateway cleanup can finish just after the client's terminal byte.
            await asyncio.sleep(settings.cleanup_timeout_seconds + 0.1)
            log_text = args.gateway_log.read_text(encoding="utf-8")
            (args.output / "gateway.log").write_text(log_text, encoding="utf-8")
        summary = write_report(args.output, log_text=log_text)
        print(
            json.dumps(
                {
                    "directory": str(args.output),
                    "outcomes": summary["outcomes"],
                    "excluded": summary["comparison_exclusion_reasons"],
                },
                ensure_ascii=False,
            )
        )
    except BaseException as exc:
        manifest.update(status="incomplete", error_type=type(exc).__name__)
        dump(args.output / "manifest.json", manifest)
        raise


def main():
    parser = argparse.ArgumentParser(description="InferScale P4 reproducible experiments")
    sub = parser.add_subparsers(dest="command", required=True)
    gen = sub.add_parser("generate")
    gen.add_argument("--workload", choices=["mixed", "shared_prefix", "hot_prefix"], required=True)
    gen.add_argument("--seed", type=int, default=42)
    gen.add_argument("--duration", type=float, default=60)
    gen.add_argument("--rate", type=float, default=2)
    gen.add_argument("--output", type=Path, required=True)
    run_parser = sub.add_parser("run")
    run_parser.add_argument("--trace", type=Path, required=True)
    run_parser.add_argument("--config", type=Path, required=True)
    run_parser.add_argument("--gateway", default="http://127.0.0.1:8000")
    run_parser.add_argument("--policy", choices=POLICIES, required=True)
    run_parser.add_argument("--output", type=Path, required=True)
    run_parser.add_argument("--mode", choices=["open_loop", "closed_loop"], default="open_loop")
    run_parser.add_argument("--capacity", type=int, default=32)
    run_parser.add_argument("--timeout", type=float, default=60)
    run_parser.add_argument("--drain", type=float, default=65)
    run_parser.add_argument("--lag-tolerance", type=float, default=0.05)
    run_parser.add_argument("--round", type=int, default=1)
    run_parser.add_argument("--gateway-log", type=Path)
    run_parser.add_argument("--environment", type=Path)
    run_parser.add_argument(
        "--cache-condition", choices=["fresh_seeded", "uncontrolled"], default="uncontrolled"
    )
    run_parser.add_argument("--ttft-slo", type=float, default=1)
    run_parser.add_argument("--e2e-slo", type=float, default=10)
    report = sub.add_parser("report")
    report.add_argument("directory", type=Path)
    report.add_argument("--gateway-log", type=Path)
    cmp = sub.add_parser("compare")
    cmp.add_argument("directories", nargs="+", type=Path)
    cmp.add_argument("--output", type=Path, required=True)
    plan = sub.add_parser("plan")
    plan.add_argument("--config", type=Path, required=True)
    plan.add_argument("--output", type=Path, required=True)
    plan.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    if args.command == "generate":
        trace = generate(args.workload, args.seed, args.duration, args.rate)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(trace.model_dump_json(indent=2), encoding="utf-8")
    elif args.command == "run":
        asyncio.run(run(args))
    elif args.command == "report":
        log_path = args.gateway_log or args.directory / "gateway.log"
        print(
            json.dumps(
                write_report(
                    args.directory,
                    log_text=log_path.read_text(encoding="utf-8") if log_path.exists() else None,
                ),
                indent=2,
            )
        )
    elif args.command == "compare":
        compare(args.directories, args.output)
    else:
        args.output.mkdir(parents=True, exist_ok=False)
        base = load_settings(args.config).model_dump(mode="json")
        for policy in POLICIES:
            config = {**base, "routing": {**base["routing"], "policy": policy}}
            (args.output / f"{policy}.yaml").write_text(
                yaml.safe_dump(config, sort_keys=False), encoding="utf-8"
            )
        rng, jobs = random.Random(args.seed), []
        for workload in ["mixed", "shared_prefix", "hot_prefix"]:
            for number in range(1, 4):
                policies = POLICIES.copy()
                rng.shuffle(policies)
                jobs.extend({"workload": workload, "round": number, "policy": p} for p in policies)
        dump(args.output / "jobs.json", {"seed": args.seed, "jobs": jobs})
        print("Wrote configs preserving native endpoints and a randomized 36-job plan.")


if __name__ == "__main__":
    main()
