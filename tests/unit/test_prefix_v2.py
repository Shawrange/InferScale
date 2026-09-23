from dataclasses import FrozenInstanceError, replace

import pytest
from pydantic import ValidationError

from inferscale.config import RoutingConfig
from inferscale.features import SuccessPrefixIndex
from inferscale.ledger import RequestLedger
from inferscale.models import Outcome
from inferscale.policies import Router
from tests.unit.test_routing_features import take


def test_history_saturation_idle_ttl_and_write_order(pool):
    registry, _, clock = pool
    a, b = registry.candidates("fake-model")
    index = SuccessPrefixIndex(
        RoutingConfig(prefix_ttl_seconds=1, prefix_max_entries=2), lambda: clock[0]
    )
    for expected in [0.25, 0.4625, 0.643125, 0.79665625, 0.9271578125, 1, 1]:
        index.remember(a, "hot")
        assert index.affinity(a, "hot") == pytest.approx(expected)
    index.remember(b, "hot")
    assert index.affinity(b, "hot") == 0.25
    before = list(index.entries.items())
    clock[0] += 0.5
    assert index.affinity(a, "hot") == 1
    assert list(index.entries.items()) == before
    index.remember(a, "other")
    assert index.affinity(a, "hot") == 0  # Reads did not refresh eviction order.
    assert len(index.entries) == 2
    clock[0] += 0.5
    assert index.affinity(b, "hot") == 0
    index.remember(b, "hot")
    assert index.affinity(b, "hot") == 0.25
    index.remember(a, None)
    assert index.affinity(a, None) == 0
    index.prune([])
    assert not index.entries


@pytest.mark.parametrize(
    "field", ["prefix_hit_increment", "prefix_history_retention", "prefix_load_soft_limit"]
)
@pytest.mark.parametrize("value", [0, -1, 1.01, float("nan"), float("inf")])
def test_invalid_v2_parameters(field, value):
    with pytest.raises(ValidationError):
        RoutingConfig(**{field: value})


@pytest.mark.parametrize("count,gate", [(0, 1), (1, 2 / 3), (2, 1 / 3), (3, 0)])
def test_gate_and_immutable_pre_reservation_score(pool, count, gate):
    registry, _, _ = pool
    worker = registry.candidates("fake-model")[0]
    router = Router(RoutingConfig(policy="prefix_v2", gamma=0.5))
    result = router.select(
        worker.model, [worker], {worker.worker_id: (count, 100)}, 20, {worker.worker_id: 0.8}
    )
    d = result.decision
    assert d.queue_ratio == count / 4
    assert d.load_gate == pytest.approx(gate)
    assert d.effective_gamma == pytest.approx(0.5 * gate)
    assert d.affinity_bonus == pytest.approx(0.4 * gate)
    assert d.final_rank == pytest.approx(count / 4 + 0.01 - 0.4 * gate)
    assert result.score == pytest.approx(d.final_rank + 0.002)
    with pytest.raises(FrozenInstanceError):
        d.queue_ratio = 1


def test_locality_spill_and_cost_can_override_locality(pool):
    registry, _, _ = pool
    a, b = registry.candidates("fake-model")
    router = Router(RoutingConfig(policy="prefix_v2", gamma=0.5))
    affinity = {a.worker_id: 1, b.worker_id: 0}

    def choose(loads):
        return router.select(a.model, [a, b], loads, 1, affinity).worker

    assert choose({a.worker_id: (0, 0), b.worker_id: (0, 0)}) == a
    assert choose({a.worker_id: (3, 0), b.worker_id: (0, 0)}) == b
    assert choose({a.worker_id: (0, 10000), b.worker_id: (0, 0)}) == b


def test_v2_gamma_zero_and_fallback(pool):
    registry, _, _ = pool
    cost = RequestLedger(registry, 8, RoutingConfig(policy="cost"))
    v2 = RequestLedger(registry, 8, RoutingConfig(policy="prefix_v2", gamma=0))
    for n, amount in enumerate([9000, 100, 200, 8000, 500, 1, 2, 300]):
        _, a = take(cost, str(n), amount)
        _, b = take(v2, str(n), amount, "hot")
        v2.remember_prefix(b, "hot")
        assert (a.worker_id, a.score) == (b.worker_id, b.score)
    router = Router(RoutingConfig(policy="prefix_v2"))
    workers = registry.candidates("fake-model")
    result = router.select(
        "fake-model", workers, {w.worker_id: (0, 0) for w in workers}, 1, {}, False
    )
    assert result.policy == "round_robin"
    assert result.fallback_reason == "untrusted_cost_state" and result.decision is None


@pytest.mark.parametrize("kind", ["epoch", "generation", "ttl"])
def test_v2_stale_completion_and_capacity(pool, kind):
    registry, _, clock = pool
    ledger = RequestLedger(
        registry, 8, RoutingConfig(policy="prefix_v2", gamma=100, prefix_ttl_seconds=1)
    )
    lease, attempt = take(ledger, "warm", 1, "hot")
    ledger.remember_prefix(attempt, "hot")
    ledger.finish(lease, Outcome.COMPLETED)
    worker = registry.snapshots()[0]
    if kind == "epoch":
        epoch = registry.replace_instance(worker.worker_id)
        registry.record_health(worker.worker_id, epoch, True, clock[0])
    elif kind == "generation":
        registry.record_health(worker.worker_id, worker.epoch, False, clock[0])
        registry.record_health(worker.worker_id, worker.epoch, True, clock[0])
    else:
        clock[0] += 1
    if kind != "ttl":
        ledger.remember_prefix(attempt, "hot")
    _, next_attempt = take(ledger, "next", 1, "hot")
    assert next_attempt.affinity == 0
    assert ledger.prefixes.affinity(replace(worker, affinity_generation=99), "hot") == 0
    for i in range(7):
        take(ledger, str(i), 1, "hot")
    assert all(w["active_attempts"] == 4 for w in ledger.snapshot()["workers"].values())
