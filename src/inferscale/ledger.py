import math
from collections import Counter
from dataclasses import dataclass, field
from uuid import uuid4

from inferscale.config import RoutingConfig
from inferscale.features import PrefixIndex, SuccessPrefixIndex
from inferscale.models import (
    CapacityExceeded,
    InvalidTransition,
    NoSchedulableWorker,
    Outcome,
    RequestContext,
    RequestLease,
    Reservation,
)
from inferscale.policies import Router
from inferscale.registry import WorkerRegistry


@dataclass
class _ActiveRequest:
    lease: RequestLease
    attempted_workers: set[str] = field(default_factory=set)
    attempts: int = 0


class RequestLedger:
    """One source of truth; no separately incremented per-worker counters.

    Methods are synchronous and short: cancellation cannot land between mutation
    and ownership registration. Never hold the shared lock across network I/O.
    """

    def __init__(
        self, registry: WorkerRegistry, global_capacity: int, routing: RoutingConfig | None = None
    ):
        if global_capacity <= 0:
            raise ValueError("global capacity must be positive")
        self.registry = registry
        self.global_capacity = global_capacity
        self._lock = registry.lock
        self._active: dict[str, _ActiveRequest] = {}
        self._reservations: dict[str, Reservation] = {}
        self._outcomes: Counter[Outcome] = Counter()
        self.routing = routing or RoutingConfig()
        self._policy = Router(self.routing)
        index = SuccessPrefixIndex if self.routing.policy == "prefix_v2" else PrefixIndex
        self.prefixes = index(self.routing, registry.clock)

    def acquire(self, context: RequestContext) -> RequestLease:
        with self._lock:
            if not context.request_id or not context.model:
                raise ValueError("request ID and model must not be empty")
            if context.request_id in self._active:
                raise InvalidTransition("request ID is already active")
            if len(self._active) >= self.global_capacity:
                raise CapacityExceeded("global capacity exhausted")
            lease = RequestLease(context, uuid4().hex)
            self._active[context.request_id] = _ActiveRequest(lease)
            return lease

    def _owned(self, lease: RequestLease) -> _ActiveRequest:
        active = self._active.get(lease.request.request_id)
        if active is None or active.lease != lease:
            raise InvalidTransition("request lease is no longer active")
        return active

    def select_and_reserve(
        self,
        lease: RequestLease,
        cost: float,
        prefix: str | None = None,
        *,
        costs_known: bool = True,
    ) -> Reservation:
        if not math.isfinite(cost) or cost < 0:
            raise ValueError("reservation cost must be finite and nonnegative")
        with self._lock:
            active = self._owned(lease)
            if any(r.lease_id == lease.lease_id for r in self._reservations.values()):
                raise InvalidTransition("release the previous attempt before retrying")
            if active.attempts >= 2:
                raise InvalidTransition("at most two attempts per request")
            healthy = self.registry.candidates(lease.request.model)
            self.prefixes.prune(healthy)
            workers = [
                w
                for w in healthy
                if w.worker_id not in active.attempted_workers
                and sum(
                    r.worker_id == w.worker_id and r.worker_epoch == w.epoch
                    for r in self._reservations.values()
                )
                < w.capacity
            ]
            if not workers:
                raise NoSchedulableWorker("no eligible replica with capacity")
            loads = {
                w.worker_id: (
                    sum(
                        r.worker_id == w.worker_id and r.worker_epoch == w.epoch
                        for r in self._reservations.values()
                    ),
                    math.fsum(
                        r.cost
                        for r in self._reservations.values()
                        if r.worker_id == w.worker_id and r.worker_epoch == w.epoch
                    ),
                )
                for w in workers
            }
            affinities = {w.worker_id: self.prefixes.affinity(w, prefix) for w in workers}
            selection = self._policy.select(
                lease.request.model,
                workers,
                loads,
                cost,
                affinities,
                costs_known,
            )
            worker, policy, score, fallback = selection
            reservation = Reservation(
                uuid4().hex,
                lease.request.request_id,
                lease.lease_id,
                worker.worker_id,
                worker.epoch,
                cost,
                active.attempts + 1,
                policy,
                score,
                affinities[worker.worker_id],
                fallback,
                worker.affinity_generation,
                selection.decision,
            )
            self._reservations[reservation.attempt_id] = reservation
            active.attempted_workers.add(worker.worker_id)
            active.attempts += 1
            return reservation

    def remember_prefix(self, reservation: Reservation, prefix: str | None):
        with self._lock:
            healthy = self.registry.candidates(
                next(
                    w.model
                    for w in self.registry.snapshots()
                    if w.worker_id == reservation.worker_id
                )
            )
            self.prefixes.prune(healthy)
            for worker in healthy:
                if (
                    worker.worker_id == reservation.worker_id
                    and worker.epoch == reservation.worker_epoch
                    and worker.affinity_generation == reservation.affinity_generation
                ):
                    self.prefixes.remember(worker, prefix)

    def release_attempt(self, reservation: Reservation) -> bool:
        with self._lock:
            if self._reservations.get(reservation.attempt_id) != reservation:
                return False
            del self._reservations[reservation.attempt_id]
            return True

    def finish(self, lease: RequestLease, outcome: Outcome) -> bool:
        """Local accounting finalizer; P2 must close upstream before invoking it."""
        outcome = Outcome(outcome)
        with self._lock:
            active = self._active.get(lease.request.request_id)
            if active is None or active.lease != lease:
                return False
            for attempt_id, reservation in tuple(self._reservations.items()):
                if reservation.lease_id == lease.lease_id:
                    del self._reservations[attempt_id]
            del self._active[lease.request.request_id]
            self._outcomes[outcome] += 1
            return True

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "active_requests": len(self._active),
                "active_attempts": len(self._reservations),
                "workers": {
                    w.worker_id: {
                        "active_attempts": sum(
                            r.worker_id == w.worker_id and r.worker_epoch == w.epoch
                            for r in self._reservations.values()
                        ),
                        "reserved_cost": math.fsum(
                            r.cost
                            for r in self._reservations.values()
                            if r.worker_id == w.worker_id and r.worker_epoch == w.epoch
                        ),
                    }
                    for w in self.registry.snapshots()
                },
                "outcomes": {outcome.value: count for outcome, count in self._outcomes.items()},
            }
