import json
import sys

import pytest

from inferscale.benchmark.__main__ import main
from inferscale.config import load_settings
from tests.unit.test_comparison_spec import area


def test_default_36_and_explicit_six_round_plan(monkeypatch):
    root = area()

    def cli(*args):
        monkeypatch.setattr(sys, "argv", ["benchmark", *map(str, args)])
        main()

    cli("plan", "--config", "configs/local.yaml", "--output", root / "default")
    jobs = json.loads((root / "default/jobs.json").read_text())["jobs"]
    assert len(jobs) == 36
    assert {j["policy"] for j in jobs} == {"round_robin", "least_load", "cost", "prefix"}
    cli("spec", "--config", "configs/local.yaml", "--output", root / "spec.json")
    cli(
        "plan",
        "--config",
        "configs/local.yaml",
        "--comparison-spec",
        root / "spec.json",
        "--output",
        root / "v2",
    )
    jobs = json.loads((root / "v2/jobs.json").read_text())["jobs"]
    assert len(jobs) == 6 and {j["workload"] for j in jobs} == {"hot_prefix"}
    original = load_settings("configs/local.yaml").model_dump(mode="json")
    for name in ["v1_g025", "v2_g050"]:
        config = load_settings(root / f"v2/{name}.yaml").model_dump(mode="json")
        assert {k: v for k, v in config.items() if k != "routing"} == {
            k: v for k, v in original.items() if k != "routing"
        }
    assert load_settings(root / "v2/v1_g025.yaml").routing.gamma == 0.25
    assert load_settings(root / "v2/v2_g050.yaml").routing.gamma == 0.5
    with pytest.raises(FileExistsError):
        cli("spec", "--config", "configs/local.yaml", "--output", root / "spec.json")
    with pytest.raises(FileExistsError):
        cli("plan", "--config", "configs/local.yaml", "--output", root / "default")
    cli(
        "plan",
        "--config",
        "configs/local.yaml",
        "--policies",
        "prefix_v2",
        "--output",
        root / "selected",
    )
    assert load_settings(root / "selected/prefix_v2.yaml").routing.policy == "prefix_v2"
