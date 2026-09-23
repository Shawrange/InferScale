import json
from copy import deepcopy
from pathlib import Path
from uuid import uuid4

import pytest
from pydantic import ValidationError

from inferscale.benchmark.compare import compare
from inferscale.benchmark.spec import ComparisonSpec
from inferscale.config import RoutingConfig, load_settings


def spec_data():
    return {
        "schema_version": 1,
        "name": "hot-v1-v2",
        "workload": "hot_prefix",
        "variants": {
            "v1_g025": RoutingConfig(policy="prefix").model_dump(),
            "v2_g050": RoutingConfig(policy="prefix_v2", gamma=0.5).model_dump(),
        },
    }


def area():
    result = Path("runs") / ("spec-test-" + uuid4().hex)
    result.mkdir(parents=True)
    return result


def run_files(root, spec, variant, number, mutate=None):
    directory = root / f"{variant}-{number}"
    directory.mkdir()
    config = load_settings("configs/local.yaml").model_dump(mode="json")
    config["routing"] = spec.variants[variant].model_dump()
    manifest = dict(
        run_id=uuid4().hex,
        status="completed",
        round=number,
        workload="hot_prefix",
        config=config,
        policy=config["routing"]["policy"],
        trace_sha256="trace",
        environment={"gpu": "fixture"},
        source_hashes={"code": "same"},
        dependencies={},
        python="3.12",
        cache_condition="fresh_seeded",
        mode="open_loop",
        capacity=32,
        client_timeout=60,
        drain_seconds=65,
        lag_tolerance=0.05,
        slo={"ttft": 1, "e2e": 10},
        variant_id=variant,
        comparison_spec_sha256=spec.digest(),
    )
    if mutate:
        mutate(manifest)
    summary = dict(
        comparison_exclusion_reasons=[],
        request_throughput=1,
        ttft_seconds={"p95": 0.1},
        e2e_seconds={"p95": 0.2},
        outcomes={"succeeded": 1},
    )
    (directory / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (directory / "summary.json").write_text(json.dumps(summary), encoding="utf-8")
    return directory


def test_explicit_groups_variants_and_default_remains_strict():
    root = area()
    spec = ComparisonSpec.model_validate(spec_data())
    dirs = [run_files(root, spec, v, n) for v in spec.variants for n in range(1, 4)]
    compare(dirs, root / "strict")
    strict = json.loads((root / "strict/comparison.json").read_text())
    assert len({r["comparison_group"] for r in strict}) == 2
    compare(dirs, root / "explicit", spec=spec)
    rows = json.loads((root / "explicit/comparison.json").read_text())
    assert len({r["comparison_group"] for r in rows}) == 1
    assert {r["variant_id"] for r in rows} == set(spec.variants)
    assert all(r["usable_rounds"] == 3 for r in rows)


@pytest.mark.parametrize("field", ["capacity", "trace", "model", "source"])
def test_spec_keeps_environment_boundaries(field):
    root = area()
    spec = ComparisonSpec.model_validate(spec_data())
    a = run_files(root, spec, "v1_g025", 1)

    def mutate(m):
        if field == "capacity":
            m["config"]["global_capacity"] += 1
        elif field == "trace":
            m["trace_sha256"] = "different"
        elif field == "model":
            m["config"]["model"]["name"] = "other"
        else:
            m["source_hashes"] = {"code": "changed"}

    b = run_files(root, spec, "v2_g050", 1, mutate)
    compare([a, b], root / "out", spec=spec)
    rows = json.loads((root / "out/comparison.json").read_text())
    assert len({r["comparison_group"] for r in rows}) == 2


def test_spec_rejects_undeclared_changes_and_incomplete_routing():
    root = area()
    spec = ComparisonSpec.model_validate(spec_data())
    bad = run_files(root, spec, "v2_g050", 1, lambda m: m["config"]["routing"].update(gamma=99))
    with pytest.raises(ValueError, match="variant"):
        compare([bad], root / "bad", spec=spec)
    data = spec_data()
    data["variants"]["v2_g050"]["cost_reference"] = 2
    with pytest.raises(ValidationError, match="common routing"):
        ComparisonSpec.model_validate(data)
    data = spec_data()
    del data["variants"]["v1_g025"]["gamma"]
    with pytest.raises(ValidationError, match="complete routing"):
        ComparisonSpec.model_validate(data)


def test_same_policy_variants_and_duplicate_rounds():
    root = area()
    data = spec_data()
    data["variants"]["v1_g050"] = deepcopy(data["variants"]["v1_g025"])
    data["variants"]["v1_g050"]["gamma"] = 0.5
    spec = ComparisonSpec.model_validate(data)
    dirs = [run_files(root, spec, v, 1) for v in spec.variants]
    compare(dirs, root / "out", spec=spec)
    assert len(json.loads((root / "out/comparison.json").read_text())) == 3
    duplicate = root / "duplicate"
    duplicate.mkdir()
    for file in dirs[0].iterdir():
        (duplicate / file.name).write_bytes(file.read_bytes())
    manifest = json.loads((duplicate / "manifest.json").read_text())
    manifest["run_id"] = "new"
    (duplicate / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="duplicate round"):
        compare([dirs[0], duplicate], root / "bad", spec=spec)
