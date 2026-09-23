import pytest
from pydantic import ValidationError

from inferscale.config import Settings
from inferscale.models import Health, NoSchedulableWorker, RequestContext


def test_stale_health_and_wrong_model_are_filtered(pool):
    registry, ledger, clock = pool
    assert len(registry.candidates("fake-model")) == 2
    assert registry.candidates("other-model") == ()
    clock[0] += 3
    assert registry.candidates("fake-model") == ()
    lease = ledger.acquire(RequestContext("stale", "fake-model"))
    with pytest.raises(NoSchedulableWorker):
        ledger.select_and_reserve(lease, 1)


def test_old_probe_cannot_restore_restarted_or_draining_worker(pool):
    registry, _, clock = pool
    old = registry.snapshots()[0]
    registry.set_draining(old.worker_id)
    assert registry.record_health(old.worker_id, old.epoch, True, clock[0])
    assert all(w.worker_id != old.worker_id for w in registry.candidates(old.model))
    epoch = registry.replace_instance(old.worker_id)
    assert not registry.record_health(old.worker_id, old.epoch, True, clock[0] + 1)
    assert registry.snapshots()[0].epoch == epoch
    assert registry.snapshots()[0].health is Health.STARTING


def test_out_of_order_probe_is_ignored_and_quarantine_is_not_probe_recovery(pool):
    registry, _, clock = pool
    worker = registry.snapshots()[0]
    clock[0] += 1
    registry.record_health(worker.worker_id, worker.epoch, False, clock[0])
    assert not registry.record_health(worker.worker_id, worker.epoch, True, clock[0] - 1)
    assert registry.snapshots()[0].health is Health.UNHEALTHY
    registry.quarantine(worker.worker_id)
    registry.record_health(worker.worker_id, worker.epoch, True, clock[0])
    assert registry.snapshots()[0].health is Health.SUSPECT


@pytest.mark.parametrize("mutation", ["duplicate_id", "duplicate_url", "model", "ttl", "unknown"])
def test_invalid_settings_fail_fast(settings, mutation):
    values = settings.model_dump(mode="json")
    if mutation == "duplicate_id":
        values["workers"][1]["worker_id"] = values["workers"][0]["worker_id"]
    elif mutation == "duplicate_url":
        values["workers"][1]["endpoint"] = values["workers"][0]["endpoint"]
    elif mutation == "model":
        values["workers"][0]["model"] = "other"
    elif mutation == "ttl":
        values["health_ttl_seconds"] = 1
    else:
        values["typo"] = 1
    with pytest.raises(ValidationError):
        Settings.model_validate(values)


@pytest.mark.parametrize(
    "url", ["file:///tmp/worker", "http://user:pass@localhost:8100", "http://localhost/v1"]
)
def test_bad_endpoint_rejected(settings, url):
    values = settings.model_dump(mode="json")
    values["workers"][0]["endpoint"] = url
    with pytest.raises(ValidationError):
        Settings.model_validate(values)
