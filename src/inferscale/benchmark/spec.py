"""Explicit treatment differences; all other comparison boundaries stay strict."""

import hashlib
import json
from pathlib import Path
from typing import Annotated, Literal

from pydantic import Field, field_validator, model_validator

from inferscale.config import RoutingConfig, StrictConfig

TREATMENT_FIELDS = frozenset(
    {
        "policy",
        "gamma",
        "prefix_hit_increment",
        "prefix_history_retention",
        "prefix_load_soft_limit",
    }
)


def common_routing(routing):
    return {k: v for k, v in routing.items() if k not in TREATMENT_FIELDS}


class ComparisonSpec(StrictConfig):
    schema_version: Literal[1]
    name: str = Field(min_length=1)
    workload: Literal["mixed", "shared_prefix", "hot_prefix"]
    variants: dict[Annotated[str, Field(pattern=r"^[a-zA-Z0-9_-]{1,64}$")], RoutingConfig] = Field(
        min_length=2
    )

    @field_validator("variants", mode="before")
    @classmethod
    def complete_routing(cls, value):
        if not isinstance(value, dict) or any(
            not isinstance(v, dict) or set(v) != set(RoutingConfig.model_fields)
            for v in value.values()
        ):
            raise ValueError("each variant must declare a complete routing configuration")
        return value

    @model_validator(mode="after")
    def treatment_only(self):
        configs = [v.model_dump(mode="json") for v in self.variants.values()]
        if any(common_routing(c) != common_routing(configs[0]) for c in configs[1:]):
            raise ValueError("common routing parameters must remain identical")
        if len({json.dumps(c, sort_keys=True) for c in configs}) != len(configs):
            raise ValueError("duplicate variant routing configuration")
        return self

    def digest(self):
        return hashlib.sha256(
            json.dumps(self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    def validate_variant(self, variant_id, routing):
        if variant_id not in self.variants or routing != self.variants[variant_id].model_dump(
            mode="json"
        ):
            raise ValueError("run routing does not exactly match declared variant")

    @classmethod
    def read(cls, path: Path):
        return cls.model_validate_json(path.read_text(encoding="utf-8"))
