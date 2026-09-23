import pytest
from pydantic import ValidationError

from inferscale.samples import RequestSample


def test_samples_round_trip_without_inventing_token_usage():
    sample = RequestSample(
        request_id="r",
        scheduled_at=0,
        sent_at=0.01,
        first_content_at=0.05,
        last_content_at=0.1,
        terminal_at=0.12,
        outcome="succeeded",
        attempt_count=2,
    )
    assert RequestSample.model_validate_json(sample.model_dump_json()) == sample
    assert sample.output_tokens is None
    assert sample.attempt_count == 2


@pytest.mark.parametrize(
    "values",
    [
        {"outcome": "not_sent", "sent_at": 1},
        {"outcome": "not_sent", "attempt_count": 1},
        {"outcome": "failed", "sent_at": None},
        {"outcome": "succeeded", "sent_at": 0.5, "output_tokens": -1},
        {"outcome": "succeeded", "sent_at": 0.5, "first_content_at": 0.6},
        {"outcome": "failed", "sent_at": 3},
    ],
)
def test_impossible_records_rejected(values):
    with pytest.raises(ValidationError):
        RequestSample(request_id="r", scheduled_at=0, terminal_at=2, **values)
