import math
from collections.abc import Sequence
from dataclasses import dataclass

from inferscale.config import RoutingConfig
from inferscale.models import RoutingDecision, WorkerSnapshot


@dataclass(frozen=True)
class Selection:
    worker: WorkerSnapshot
    policy: str
    score: float
    fallback_reason: str | None
    decision: RoutingDecision | None

    def __iter__(self):
        # Preserve the existing four-value unpacking API.
        return iter((self.worker, self.policy, self.score, self.fallback_reason))


class RoundRobin:
    def __init__(self):
        self._last: dict[str, str] = {}

    def select(self, model: str, candidates: Sequence[WorkerSnapshot]) -> WorkerSnapshot:
        """Called only under the shared registry/ledger lock, with nonempty candidates."""
        ordered = sorted(candidates, key=lambda w: w.worker_id)
        last = self._last.get(model, "")
        worker = next((w for w in ordered if w.worker_id > last), ordered[0])
        self._last[model] = worker.worker_id
        return worker


class Router:
    def __init__(self, config: RoutingConfig):
        self.config = config
        self.ties = RoundRobin()

    def select(self, model, candidates, loads, cost, affinities, costs_known=True):
        policy = self.config.policy
        fallback = None
        if policy in {"cost", "prefix", "prefix_v2"} and not costs_known:
            policy, fallback = "round_robin", "untrusted_cost_state"
        scores, ranks = {}, {}
        decisions = {}
        for worker in candidates:
            count, outstanding = loads[worker.worker_id]
            q = count / worker.capacity
            score = 0.0
            if policy == "least_load":
                score = q
            elif policy in {"cost", "prefix", "prefix_v2"}:
                score = self.config.queue_weight * q + self.config.cost_weight * (
                    outstanding / self.config.cost_reference
                )
                if policy == "prefix":
                    score -= self.config.gamma * affinities[worker.worker_id]
                elif policy == "prefix_v2":
                    base = score
                    gate = max(0.0, 1.0 - q / self.config.prefix_load_soft_limit)
                    effective = self.config.gamma * gate
                    bonus = effective * affinities[worker.worker_id]
                    score -= bonus
                    decisions[worker.worker_id] = RoutingDecision(
                        q, base, gate, effective, bonus, score
                    )
            ranks[worker.worker_id] = score
            if policy in {"cost", "prefix", "prefix_v2"}:
                # The current request is a common term in a homogeneous pool.
                # Exclude it from comparisons to avoid erasing differences when large.
                score += self.config.cost_weight * (cost / self.config.cost_reference)
            if not math.isfinite(score):
                raise ValueError("routing score must be finite")
            scores[worker.worker_id] = score
        minimum = min(ranks.values())
        tied = [w for w in candidates if ranks[w.worker_id] == minimum]
        chosen = self.ties.select(model, tied)
        return Selection(
            chosen, policy, scores[chosen.worker_id], fallback, decisions.get(chosen.worker_id)
        )
