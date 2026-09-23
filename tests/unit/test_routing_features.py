from dataclasses import replace

import pytest

from inferscale.config import RoutingConfig
from inferscale.features import OutputPredictor, fingerprint, fixture_token_count, fixture_token_ids
from inferscale.ledger import RequestLedger
from inferscale.models import Outcome, RequestContext
from inferscale.policies import Router


def take(ledger, name, cost, prefix=None):
    lease = ledger.acquire(RequestContext(name, "fake-model"))
    return lease, ledger.select_and_reserve(lease, cost, prefix)


@pytest.mark.parametrize("cost", [1, 1000, 50000, 1e100])
def test_c1_c2_hand_scores_and_common_current_cost(pool, cost):
    registry, _, _ = pool
    router = Router(RoutingConfig(policy="cost"))
    candidates = registry.candidates("fake-model")
    worker, policy, score, reason = router.select(
        "fake-model",
        candidates,
        {"worker-0": (2, 9000), "worker-1": (2, 1000)},
        cost,
        {"worker-0": 0, "worker-1": 0},
    )
    assert worker.worker_id == "worker-1"
    assert score == pytest.approx(0.5 + (1000 + cost) / 10000)
    assert policy == "cost" and reason is None


def test_c3_atomic_reservation_changes_next_decision(pool):
    registry, _, _ = pool
    ledger = RequestLedger(registry, 8, RoutingConfig(policy="cost", queue_weight=0))
    _, first = take(ledger, "large", 9000)
    _, second = take(ledger, "small", 1000)
    assert first.worker_id != second.worker_id
    assert ledger.snapshot()["workers"][first.worker_id]["reserved_cost"] == 9000
    assert ledger.release_attempt(first)
    assert ledger.snapshot()["workers"][first.worker_id]["reserved_cost"] == 0


def test_c4_prediction_change_does_not_reprice_reservation(pool, settings):
    registry, _, _ = pool
    config = RoutingConfig(policy="cost", ewma_alpha=1)
    ledger = RequestLedger(registry, 8, config)
    predictor = OutputPredictor(settings.model, config)
    features = predictor.estimate(100, 200, None)
    lease, attempt = take(ledger, "one", features.cost)
    assert features.cost == 164
    assert predictor.observe(
        features, {"prompt_tokens": 100, "completion_tokens": 150, "total_tokens": 250}, "stop"
    )
    assert predictor.estimate(100, 200, None).cost == 250
    assert attempt.cost == 164
    assert ledger.release_attempt(attempt)
    assert ledger.finish(lease, Outcome.COMPLETED)
    assert all(w["reserved_cost"] == 0 for w in ledger.snapshot()["workers"].values())


@pytest.mark.parametrize(
    "usage,reason",
    [
        (None, "stop"),
        ({}, "stop"),
        ({"prompt_tokens": 99, "completion_tokens": 1, "total_tokens": 100}, "stop"),
        ({"prompt_tokens": 100, "completion_tokens": True, "total_tokens": 101}, "stop"),
        ({"prompt_tokens": 100, "completion_tokens": 201, "total_tokens": 301}, "stop"),
        ({"prompt_tokens": 100, "completion_tokens": 1, "total_tokens": 102}, "stop"),
        ({"prompt_tokens": 100, "completion_tokens": 1, "total_tokens": 101}, "content_filter"),
    ],
)
def test_c5_untrusted_usage_does_not_update(settings, usage, reason):
    predictor = OutputPredictor(settings.model, RoutingConfig())
    assert not predictor.observe(predictor.estimate(100, 200, None), usage, reason)
    assert predictor.buckets == {}


def test_ewma_bucket_cap_and_censored_flag(settings):
    predictor = OutputPredictor(settings.model, RoutingConfig(ewma_alpha=0.5))
    features = predictor.estimate(100, 10, None)
    assert features.output_estimate == 10
    predictor.observe(
        features, {"prompt_tokens": 100, "completion_tokens": 10, "total_tokens": 110}, "length"
    )
    assert predictor.estimate(110, 100, None).output_estimate == 37
    assert predictor.estimate(200, 100, None).output_estimate == 64
    assert predictor.length_capped == 1


def test_least_load_normalizes_capacity_and_rotates_ties(pool):
    registry, _, _ = pool
    a, b = registry.candidates("fake-model")
    router = Router(RoutingConfig(policy="least_load"))
    candidates = [replace(a, capacity=2), b]
    loads = {a.worker_id: (1, 0), b.worker_id: (1, 9000)}
    chosen, *_ = router.select(a.model, candidates, loads, 1, {})
    assert chosen == b
    loads[b.worker_id] = (2, 9000)
    chosen, *_ = router.select(a.model, candidates, loads, 1, {})
    assert chosen.worker_id == a.worker_id


def test_cost_falls_back_to_rr_when_state_untrusted(pool):
    registry, _, _ = pool
    ledger = RequestLedger(registry, 8, RoutingConfig(policy="cost"))
    lease = ledger.acquire(RequestContext("unknown", "fake-model"))
    attempt = ledger.select_and_reserve(lease, 0, costs_known=False)
    assert attempt.policy == "round_robin"
    assert attempt.fallback_reason == "untrusted_cost_state"


def test_gamma_zero_matches_cost_sequence(pool):
    registry, _, _ = pool
    cost = RequestLedger(registry, 8, RoutingConfig(policy="cost"))
    prefix = RequestLedger(registry, 8, RoutingConfig(policy="prefix", gamma=0))
    for index, amount in enumerate([9000, 100, 200, 8000, 500, 1, 2, 300]):
        _, a = take(cost, str(index), amount)
        _, b = take(prefix, str(index), amount, "hot")
        prefix.remember_prefix(b, "hot")
        assert a.worker_id == b.worker_id


def test_prefix_affinity_and_capacity_gate(pool):
    registry, _, _ = pool
    ledger = RequestLedger(registry, 8, RoutingConfig(policy="prefix", gamma=100))
    lease, warm = take(ledger, "warm", 1, "hot")
    ledger.remember_prefix(warm, "hot")
    ledger.finish(lease, Outcome.COMPLETED)
    attempts = [take(ledger, str(i), 1, "hot")[1] for i in range(5)]
    assert all(a.worker_id == warm.worker_id for a in attempts[:4])
    assert attempts[-1].worker_id != warm.worker_id
    assert attempts[0].affinity == 1


@pytest.mark.parametrize("invalidate", ["failure", "restart", "stale", "ttl"])
def test_prefix_invalidation_and_old_completion_cannot_restore(pool, invalidate):
    registry, _, clock = pool
    ledger = RequestLedger(registry, 8, RoutingConfig(policy="prefix", prefix_ttl_seconds=1))
    lease, warm = take(ledger, "warm", 1, "hot")
    ledger.remember_prefix(warm, "hot")
    ledger.finish(lease, Outcome.COMPLETED)
    old = registry.snapshots()[0]
    if invalidate == "failure":
        registry.record_health(old.worker_id, old.epoch, False, clock[0])
        registry.record_health(old.worker_id, old.epoch, True, clock[0])
    elif invalidate == "restart":
        epoch = registry.replace_instance(old.worker_id)
        registry.record_health(old.worker_id, epoch, True, clock[0])
    elif invalidate == "stale":
        clock[0] += 3
        for worker in registry.snapshots():
            registry.record_health(worker.worker_id, worker.epoch, True, clock[0])
    else:
        clock[0] += 1
    if invalidate != "ttl":
        ledger.remember_prefix(warm, "hot")
    _, attempt = take(ledger, "next", 1, "hot")
    assert attempt.affinity == 0


def test_prefix_bounded_and_model_identity_separated(pool, settings):
    registry, _, _ = pool
    ledger = RequestLedger(registry, 8, RoutingConfig(prefix_max_entries=2))
    _, attempt = take(ledger, "one", 1)
    for key in ["a", "b", "c"]:
        ledger.remember_prefix(attempt, key)
    assert len(ledger.prefixes.entries) == 2
    ids = tuple(range(100))
    model = settings.model
    assert fingerprint(model, ids, 101) is None
    assert fingerprint(model, ids, 32) == fingerprint(model, ids[:32] + (999,), 32)
    for field in ["name", "tokenizer_identity", "template_identity"]:
        assert fingerprint(model, ids, 32) != fingerprint(
            model.model_copy(update={field: "new"}), ids, 32
        )


@pytest.mark.parametrize("chat", [True, False])
def test_fixture_ids_preserve_fixture_count(chat):
    payload = {"messages": [{"role": "user", "content": "世界"}], "prompt": "世界"}
    assert len(fixture_token_ids(payload, chat)) == fixture_token_count(payload, chat)
