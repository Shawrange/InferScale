import hashlib
import json
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass

from inferscale.config import ModelSpec, RoutingConfig

TokenCounter = Callable[[dict, bool], int]


def fixture_token_ids(payload: dict, chat: bool) -> tuple[int, ...]:
    if not chat:
        return (256, *payload["prompt"].encode("utf-8"))
    ids = [257, 258]
    for message in payload["messages"]:
        ids.extend([259, {"system": 260, "user": 261, "assistant": 262}[message["role"]], 263, 264])
        ids.extend(message["content"].encode("utf-8"))
    return tuple(ids)


def fingerprint(model: ModelSpec, ids: tuple[int, ...], size: int) -> str | None:
    if len(ids) < size:
        return None
    identity = [model.name, model.tokenizer_identity, model.template_identity, list(ids[:size])]
    return hashlib.sha256(json.dumps(identity, separators=(",", ":")).encode()).hexdigest()


@dataclass(frozen=True)
class Features:
    prompt_tokens: int
    output_cap: int
    output_estimate: float
    cost: float
    prefix: str | None


class OutputPredictor:
    """Fixed model, logarithmic prompt buckets; callers hold registry.lock."""

    def __init__(self, model: ModelSpec, config: RoutingConfig):
        self.model, self.config = model, config
        self.buckets: dict[int, float] = {}
        self.updates = 0
        self.length_capped = 0

    def estimate(self, prompt_tokens: int, cap: int, prefix: str | None) -> Features:
        predicted = self.buckets.get(prompt_tokens.bit_length(), self.model.default_output_tokens)
        output = min(cap, predicted)
        cost = self.config.prompt_weight * prompt_tokens + self.config.output_weight * output
        return Features(prompt_tokens, cap, output, cost, prefix)

    def observe(self, features: Features, usage, finish_reason: str | None) -> bool:
        if not isinstance(usage, dict) or finish_reason not in {"stop", "length"}:
            return False
        prompt, output = usage.get("prompt_tokens"), usage.get("completion_tokens")
        total = usage.get("total_tokens")
        if (
            type(prompt) is not int
            or type(output) is not int
            or type(total) is not int
            or prompt != features.prompt_tokens
            or not 0 <= output <= features.output_cap
            or total != prompt + output
        ):
            return False
        bucket = prompt.bit_length()
        old = self.buckets.get(bucket, self.model.default_output_tokens)
        self.buckets[bucket] = self.config.ewma_alpha * output + (1 - self.config.ewma_alpha) * old
        self.updates += 1
        self.length_capped += int(finish_reason == "length" or output == features.output_cap)
        return True


class PrefixIndex:
    """Binary affinity heuristic, not a backend cache-hit measurement."""

    def __init__(self, config: RoutingConfig, clock):
        self.config, self.clock = config, clock
        self.entries: OrderedDict[tuple, float] = OrderedDict()

    @staticmethod
    def key(worker, prefix):
        return (worker.worker_id, worker.epoch, worker.affinity_generation, prefix)

    def prune(self, healthy_workers):
        valid = {(w.worker_id, w.epoch, w.affinity_generation) for w in healthy_workers}
        now = self.clock()
        for key, expires in tuple(self.entries.items()):
            if key[:3] not in valid or expires <= now:
                del self.entries[key]

    def affinity(self, worker, prefix) -> float:
        if prefix is None:
            return 0.0
        return float(self.entries.get(self.key(worker, prefix), 0) > self.clock())

    def remember(self, worker, prefix):
        if prefix is None:
            return
        key = self.key(worker, prefix)
        self.entries[key] = self.clock() + self.config.prefix_ttl_seconds
        self.entries.move_to_end(key)
        while len(self.entries) > self.config.prefix_max_entries:
            self.entries.popitem(last=False)


def fixture_token_count(payload: dict, chat: bool) -> int:
    """Deterministic fake units only. This is NOT an LLM tokenizer.

    The same rule runs inside the fake worker so context checks are testable.
    A real model requires an explicitly supplied matching tokenizer in P3.
    """
    if chat:
        return 2 + sum(4 + len(m["content"].encode("utf-8")) for m in payload["messages"])
    return 1 + len(payload["prompt"].encode("utf-8"))
