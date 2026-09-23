import asyncio
from concurrent.futures import ThreadPoolExecutor

import pytest

from inferscale.ledger import RequestLedger
from inferscale.lifecycle import request_scope
from inferscale.models import (
    CapacityExceeded,
    InvalidTransition,
    NoSchedulableWorker,
    Outcome,
    RequestContext,
)


def context(number=0):
    return RequestContext(f"request-{number}", "fake-model")


def assert_empty(ledger):
    snapshot = ledger.snapshot()
    assert snapshot["active_requests"] == snapshot["active_attempts"] == 0
    assert all(
        w["reserved_cost"] == w["active_attempts"] == 0 for w in snapshot["workers"].values()
    )


def test_rr_burst_reserves_without_waiting_for_next_probe(pool):
    _, ledger, _ = pool
    leases = [ledger.acquire(context(i)) for i in range(8)]
    attempts = [ledger.select_and_reserve(lease, 1000) for lease in leases]
    assert [a.worker_id for a in attempts] == ["worker-0", "worker-1"] * 4
    assert ledger.snapshot()["workers"] == {
        "worker-0": {"active_attempts": 4, "reserved_cost": 4000},
        "worker-1": {"active_attempts": 4, "reserved_cost": 4000},
    }
    for lease in leases:
        ledger.finish(lease, Outcome.COMPLETED)
    assert_empty(ledger)


def test_global_admission_is_atomic_under_thread_contention(pool):
    _, ledger, _ = pool

    def compete(i):
        try:
            return ledger.acquire(context(i))
        except CapacityExceeded:
            return None

    with ThreadPoolExecutor(max_workers=16) as executor:
        leases = [lease for lease in executor.map(compete, range(100)) if lease]
    assert len(leases) == ledger.snapshot()["active_requests"] == 8
    for lease in leases:
        ledger.finish(lease, Outcome.COMPLETED)
    assert_empty(ledger)


def test_per_worker_capacity_and_selection_are_atomic(pool):
    registry, _, _ = pool
    ledger = RequestLedger(registry, global_capacity=40)
    leases = [ledger.acquire(context(i)) for i in range(40)]

    def reserve(lease):
        try:
            return ledger.select_and_reserve(lease, 64)
        except NoSchedulableWorker:
            return None

    with ThreadPoolExecutor(max_workers=16) as executor:
        attempts = [a for a in executor.map(reserve, leases) if a]
    assert len(attempts) == 8
    assert all(w["active_attempts"] == 4 for w in ledger.snapshot()["workers"].values())
    for lease in leases:
        ledger.finish(lease, Outcome.COMPLETED)
    assert_empty(ledger)


def test_retry_preserves_one_request_and_releases_exact_original_cost(pool):
    _, ledger, _ = pool
    lease = ledger.acquire(context())
    predicted_cost = 9000
    a = ledger.select_and_reserve(lease, predicted_cost)
    predicted_cost = 1000  # A later predictor update must not change the old reservation.
    with pytest.raises(InvalidTransition):
        ledger.select_and_reserve(lease, predicted_cost)
    assert ledger.release_attempt(a)
    assert not ledger.release_attempt(a)
    b = ledger.select_and_reserve(lease, predicted_cost)
    assert a.worker_id != b.worker_id
    assert (a.attempt_number, b.attempt_number) == (1, 2)
    assert ledger.snapshot()["active_requests"] == 1
    assert ledger.snapshot()["workers"][a.worker_id]["reserved_cost"] == 0
    assert ledger.snapshot()["workers"][b.worker_id]["reserved_cost"] == 1000
    assert not ledger.release_attempt(a)
    assert ledger.release_attempt(b)
    with pytest.raises(InvalidTransition):
        ledger.select_and_reserve(lease, 1)
    assert ledger.finish(lease, Outcome.COMPLETED)
    assert not ledger.finish(lease, Outcome.CANCELLED)
    assert ledger.snapshot()["outcomes"] == {"completed": 1}
    assert_empty(ledger)


def test_stale_finalizer_cannot_release_reused_request_id(pool):
    _, ledger, _ = pool
    old = ledger.acquire(context())
    ledger.finish(old, Outcome.COMPLETED)
    new = ledger.acquire(context())
    new_attempt = ledger.select_and_reserve(new, 512)
    assert not ledger.finish(old, Outcome.FAILED)
    assert ledger.snapshot()["active_attempts"] == 1
    assert ledger.release_attempt(new_attempt)
    ledger.finish(new, Outcome.COMPLETED)
    assert_empty(ledger)


def test_duplicate_request_is_not_admitted(pool):
    _, ledger, _ = pool
    lease = ledger.acquire(context())
    with pytest.raises(InvalidTransition):
        ledger.acquire(context())
    ledger.finish(lease, Outcome.COMPLETED)


@pytest.mark.parametrize("cost", [-1, float("nan"), float("inf")])
def test_invalid_cost_does_not_create_attempt(pool, cost):
    _, ledger, _ = pool
    with request_scope(ledger, context()) as lease:
        with pytest.raises(ValueError):
            ledger.select_and_reserve(lease, cost)
        assert ledger.snapshot()["active_attempts"] == 0
    assert_empty(ledger)


@pytest.mark.parametrize("failure", [ValueError, ConnectionError, TimeoutError])
def test_scope_recovers_unreleased_reservation_on_exception(pool, failure):
    _, ledger, _ = pool
    with pytest.raises(failure), request_scope(ledger, context()) as lease:
        ledger.select_and_reserve(lease, 128)
        raise failure("injected")
    assert_empty(ledger)


async def test_cancel_after_reserve_before_caller_keeps_handle(pool):
    _, ledger, _ = pool
    reserved = asyncio.Event()

    async def run():
        with request_scope(ledger, context()) as lease:
            ledger.select_and_reserve(lease, 1024)  # Intentionally discard attempt handle.
            reserved.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(run())
    await reserved.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert ledger.snapshot()["outcomes"] == {"cancelled": 1}
    assert_empty(ledger)


def test_no_worker_scope_releases_admission(pool):
    registry, ledger, _ = pool
    for worker in registry.snapshots():
        registry.set_draining(worker.worker_id)
    with pytest.raises(NoSchedulableWorker), request_scope(ledger, context()) as lease:
        ledger.select_and_reserve(lease, 42)
    assert ledger.snapshot()["outcomes"] == {"rejected": 1}
    assert_empty(ledger)


def test_old_epoch_release_does_not_subtract_new_instance_cost(pool):
    registry, ledger, clock = pool
    lease_a = ledger.acquire(context("a"))
    old = ledger.select_and_reserve(lease_a, 9000)
    registry.set_draining("worker-1")
    epoch = registry.replace_instance(old.worker_id)
    registry.record_health(old.worker_id, epoch, True, clock[0])
    lease_b = ledger.acquire(context("b"))
    new = ledger.select_and_reserve(lease_b, 512)
    ledger.finish(lease_a, Outcome.FAILED)
    assert ledger.snapshot()["workers"][new.worker_id] == {
        "active_attempts": 1,
        "reserved_cost": 512,
    }
    ledger.finish(lease_b, Outcome.COMPLETED)
    assert_empty(ledger)
