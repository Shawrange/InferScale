import pytest

from inferscale.config import Settings, load_settings
from inferscale.ledger import RequestLedger
from inferscale.registry import WorkerRegistry


@pytest.fixture
def settings() -> Settings:
    return load_settings("configs/local.yaml")


@pytest.fixture
def pool(settings):
    clock = [100.0]
    registry = WorkerRegistry(settings.workers, 3, clock=lambda: clock[0])
    for worker in registry.snapshots():
        registry.record_health(worker.worker_id, worker.epoch, True, clock[0])
    return registry, RequestLedger(registry, settings.global_capacity), clock
