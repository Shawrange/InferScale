from dataclasses import dataclass
from enum import StrEnum


class Health(StrEnum):
    STARTING = "starting"
    HEALTHY = "healthy"
    SUSPECT = "suspect"
    UNHEALTHY = "unhealthy"


class Outcome(StrEnum):
    COMPLETED = "completed"
    REJECTED = "rejected"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


@dataclass(frozen=True, slots=True)
class RequestContext:
    request_id: str
    model: str


@dataclass(frozen=True, slots=True)
class RequestLease:
    request: RequestContext
    lease_id: str


@dataclass(frozen=True, slots=True)
class RoutingDecision:
    queue_ratio: float
    base_score: float
    load_gate: float
    effective_gamma: float
    affinity_bonus: float
    final_rank: float


@dataclass(frozen=True, slots=True)
class Reservation:
    attempt_id: str
    request_id: str
    lease_id: str
    worker_id: str
    worker_epoch: str
    cost: float
    attempt_number: int
    policy: str = "round_robin"
    score: float | None = None
    affinity: float = 0
    fallback_reason: str | None = None
    affinity_generation: int = 0
    decision: RoutingDecision | None = None


@dataclass(frozen=True, slots=True)
class WorkerSnapshot:
    worker_id: str
    endpoint: str
    model: str
    capacity: int
    epoch: str
    health: Health = Health.STARTING
    checked_at: float | None = None
    draining: bool = False
    affinity_generation: int = 0


class CapacityExceeded(Exception):
    """Global admission limit reached; do not enqueue."""


class NoSchedulableWorker(Exception):
    """No healthy, fresh worker with local capacity."""


class InvalidTransition(Exception):
    """The request/attempt ownership or lifecycle is invalid."""
