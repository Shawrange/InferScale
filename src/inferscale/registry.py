from collections.abc import Callable, Sequence
from dataclasses import replace
from threading import RLock
from time import monotonic
from uuid import uuid4

from inferscale.config import WorkerConfig
from inferscale.models import Health, WorkerSnapshot


class WorkerRegistry:
    """Static membership. The ledger shares this short synchronous lock."""

    def __init__(
        self,
        workers: Sequence[WorkerConfig],
        health_ttl: float,
        clock: Callable[[], float] = monotonic,
    ):
        self.lock = RLock()
        self.clock = clock
        self.health_ttl = health_ttl
        self._workers = {
            w.worker_id: WorkerSnapshot(
                w.worker_id, str(w.endpoint).rstrip("/"), w.model, w.capacity, uuid4().hex
            )
            for w in workers
        }

    def snapshots(self) -> tuple[WorkerSnapshot, ...]:
        with self.lock:
            return tuple(self._workers.values())

    def candidates(self, model: str) -> tuple[WorkerSnapshot, ...]:
        with self.lock:
            now = self.clock()
            return tuple(
                w
                for w in self._workers.values()
                if w.model == model
                and w.health is Health.HEALTHY
                and not w.draining
                and w.checked_at is not None
                and 0 <= now - w.checked_at < self.health_ttl
            )

    def record_health(self, worker_id: str, epoch: str, healthy: bool, observed_at: float):
        with self.lock:
            worker = self._workers[worker_id]
            if worker.epoch != epoch:
                return False
            if worker.checked_at is not None and observed_at < worker.checked_at:
                return False
            # A successful health probe alone cannot clear an uncertain-abort quarantine.
            health = Health.HEALTHY if healthy else Health.UNHEALTHY
            if worker.health is Health.SUSPECT:
                health = Health.SUSPECT
            invalidated = (
                not healthy
                or worker.health is not Health.HEALTHY
                or worker.checked_at is None
                or observed_at - worker.checked_at >= self.health_ttl
            )
            self._workers[worker_id] = replace(
                worker,
                health=health,
                checked_at=observed_at,
                affinity_generation=worker.affinity_generation + int(invalidated),
            )
            return True

    def quarantine(self, worker_id: str, epoch: str | None = None):
        with self.lock:
            worker = self._workers[worker_id]
            if epoch is not None and worker.epoch != epoch:
                return False
            self._workers[worker_id] = replace(
                worker,
                health=Health.SUSPECT,
                affinity_generation=worker.affinity_generation + 1,
            )
            return True

    def set_draining(self, worker_id: str, draining: bool = True):
        with self.lock:
            worker = self._workers[worker_id]
            self._workers[worker_id] = replace(
                worker,
                draining=draining,
                affinity_generation=worker.affinity_generation + int(draining),
            )

    def replace_instance(self, worker_id: str) -> str:
        """Explicit confirmed restart, not inferred from a successful health probe."""
        with self.lock:
            worker = self._workers[worker_id]
            epoch = uuid4().hex
            self._workers[worker_id] = replace(
                worker, epoch=epoch, health=Health.STARTING, checked_at=None
            )
            return epoch
