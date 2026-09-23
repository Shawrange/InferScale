from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator, model_validator


class StrictConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class ModelSpec(StrictConfig):
    name: str = Field(min_length=1)
    tokenizer_identity: str = Field(min_length=1)
    template_identity: str = Field(min_length=1)
    context_limit: int = Field(gt=0)
    default_output_tokens: int = Field(gt=0)

    @model_validator(mode="after")
    def output_fits_context(self):
        if self.default_output_tokens >= self.context_limit:
            raise ValueError("default output must leave space for prompt tokens")
        return self


class WorkerConfig(StrictConfig):
    worker_id: str = Field(pattern=r"^[a-zA-Z0-9_-]{1,64}$")
    endpoint: HttpUrl
    model: str = Field(min_length=1)
    capacity: int = Field(gt=0)

    @field_validator("endpoint")
    @classmethod
    def base_endpoint_only(cls, value):
        if value.username or value.password or value.query or value.fragment:
            raise ValueError("worker endpoint cannot contain credentials, query or fragment")
        if value.path not in (None, "", "/"):
            raise ValueError("worker endpoint must be a base URL without a path")
        return value


class RoutingConfig(StrictConfig):
    policy: Literal["round_robin", "least_load", "cost", "prefix"] = "round_robin"
    prompt_weight: float = Field(default=1, ge=0)
    output_weight: float = Field(default=1, ge=0)
    queue_weight: float = Field(default=1, ge=0)
    cost_weight: float = Field(default=1, gt=0)
    cost_reference: float = Field(default=10000, gt=0)
    ewma_alpha: float = Field(default=0.2, gt=0, le=1)
    prefix_tokens: int = Field(default=32, gt=0)
    prefix_ttl_seconds: float = Field(default=300, gt=0)
    prefix_max_entries: int = Field(default=1024, gt=0)
    gamma: float = Field(default=0.25, ge=0)

    @model_validator(mode="after")
    def nonzero_cost(self):
        if self.prompt_weight == self.output_weight == 0:
            raise ValueError("at least one token cost weight must be positive")
        return self


class Settings(StrictConfig):
    model: ModelSpec
    workers: tuple[WorkerConfig, ...] = Field(min_length=1)
    global_capacity: int = Field(default=8, gt=0)
    health_interval_seconds: float = Field(default=1, gt=0)
    health_timeout_seconds: float = Field(default=0.5, gt=0)
    health_ttl_seconds: float = Field(default=3, gt=0)
    max_body_bytes: int = Field(default=1_048_576, gt=0)
    max_response_bytes: int = Field(default=4_194_304, gt=0)
    max_event_bytes: int = Field(default=65_536, gt=0)
    max_prefetch_bytes: int = Field(default=131_072, gt=0)
    max_output_tokens: int = Field(default=2048, gt=0)
    body_timeout_seconds: float = Field(default=5, gt=0)
    connect_timeout_seconds: float = Field(default=2, gt=0)
    pool_timeout_seconds: float = Field(default=1, gt=0)
    first_output_timeout_seconds: float = Field(default=30, gt=0)
    idle_timeout_seconds: float = Field(default=30, gt=0)
    overall_timeout_seconds: float = Field(default=60, gt=0)
    downstream_timeout_seconds: float = Field(default=5, gt=0)
    cleanup_timeout_seconds: float = Field(default=1, gt=0)
    api_key_env: str | None = None
    routing: RoutingConfig = Field(default_factory=RoutingConfig)
    tokenizer_slots: int = Field(default=2, gt=0)
    tokenizer_mode: Literal["fixture", "vllm"] = "fixture"
    tokenizer_timeout_seconds: float = Field(default=5, gt=0)

    @model_validator(mode="after")
    def validate_pool(self):
        if len({w.worker_id for w in self.workers}) != len(self.workers):
            raise ValueError("duplicate worker_id")
        if len({str(w.endpoint) for w in self.workers}) != len(self.workers):
            raise ValueError("duplicate worker endpoint would count one replica twice")
        if any(w.model != self.model.name for w in self.workers):
            raise ValueError("this version supports exactly one model pool")
        if self.health_ttl_seconds <= self.health_interval_seconds + self.health_timeout_seconds:
            raise ValueError("health TTL must exceed interval + probe timeout")
        return self


def load_settings(path: str | Path) -> Settings:
    with Path(path).open(encoding="utf-8") as file:
        return Settings.model_validate(yaml.safe_load(file))
