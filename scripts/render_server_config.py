"""Prepare a real-backend config without downloading model files or changing the host."""

import argparse
from pathlib import Path

import yaml

from inferscale.config import Settings, load_settings


def render(identity, policy, context_limit):
    values = load_settings("configs/local.yaml").model_dump(mode="json")
    values["model"].update(
        name="inference-model",
        tokenizer_identity=identity,
        template_identity=identity,
        context_limit=context_limit,
    )
    values.update(tokenizer_mode="vllm", max_output_tokens=min(512, context_limit - 1))
    values["routing"]["policy"] = policy
    values["routing"]["prefix_tokens"] = 32
    for index, worker in enumerate(values["workers"]):
        worker.update(model="inference-model", endpoint=f"http://worker-{index}:8000")
    return Settings.model_validate(values)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--model-identity",
        required=True,
        help="Immutable model/tokenizer/template snapshot identity, e.g. repo@commit",
    )
    parser.add_argument(
        "--policy", choices=["round_robin", "least_load", "cost", "prefix"], default="round_robin"
    )
    parser.add_argument("--context-limit", type=int, default=2048)
    args = parser.parse_args()
    config = render(args.model_identity, args.policy, args.context_limit)
    target = Path("configs/server.yaml")
    target.write_text(
        yaml.safe_dump(config.model_dump(mode="json"), sort_keys=False), encoding="utf-8"
    )
    print(
        f"Wrote {target}; match CONTEXT_LIMIT in deploy/gpu.env. Restart gateway to change policy."
    )
