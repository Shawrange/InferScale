"""In-memory accounting demo, no network or GPU. Real socket tests live under tests/."""

import json

from inferscale.config import load_settings
from inferscale.ledger import RequestLedger
from inferscale.lifecycle import request_scope
from inferscale.models import RequestContext
from inferscale.registry import WorkerRegistry


def main():
    settings = load_settings("configs/local.yaml")
    registry = WorkerRegistry(settings.workers, settings.health_ttl_seconds)
    # Explicit fixture: this demo has no actual running workers.
    for worker in registry.snapshots():
        registry.record_health(worker.worker_id, worker.epoch, True, registry.clock())
    ledger = RequestLedger(registry, settings.global_capacity)
    with request_scope(ledger, RequestContext("demo", settings.model.name)) as lease:
        first = ledger.select_and_reserve(lease, cost=9000)
        print("Attempt 1:", first.worker_id, "cost:", first.cost)
        print(json.dumps(ledger.snapshot(), indent=2))
        ledger.release_attempt(first)
        second = ledger.select_and_reserve(lease, cost=1000)
        print("Attempt 2:", second.worker_id, "cost:", second.cost)
        print(json.dumps(ledger.snapshot(), indent=2))
    print("After finalization:")
    print(json.dumps(ledger.snapshot(), indent=2))


if __name__ == "__main__":
    main()
