"""Client clock records; gateway attempt timings are kept separately."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class RequestSample(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    request_id: str = Field(min_length=1)
    scheduled_at: float = Field(ge=0)
    sent_at: float | None = Field(default=None, ge=0)
    first_content_at: float | None = Field(default=None, ge=0)
    last_content_at: float | None = Field(default=None, ge=0)
    terminal_at: float = Field(ge=0)
    outcome: Literal["not_sent", "succeeded", "rejected", "failed", "cancelled"]
    error_subtype: str | None = None
    output_tokens: int | None = Field(default=None, ge=0)
    attempt_count: int | None = Field(default=None, ge=0, le=2)
    worker_id: str | None = None
    gateway_request_id: str | None = None
    prompt_tokens: int | None = Field(default=None, ge=0)
    finish_reason: str | None = None
    chunk_intervals: tuple[Annotated[float, Field(ge=0)], ...] = ()
    group: str = "unspecified"
    trace_index: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def validate_times(self):
        if (self.outcome == "not_sent") != (self.sent_at is None):
            raise ValueError("only not_sent records omit sent_at")
        if (self.first_content_at is None) != (self.last_content_at is None):
            raise ValueError("first and last content timestamps must both be present or absent")
        if self.sent_at is None and (
            self.first_content_at is not None
            or self.output_tokens is not None
            or self.attempt_count
        ):
            raise ValueError("not_sent cannot contain output or attempts")
        times = [
            t
            for t in (
                self.scheduled_at,
                self.sent_at,
                self.first_content_at,
                self.last_content_at,
                self.terminal_at,
            )
            if t is not None
        ]
        if times != sorted(times):
            raise ValueError("timestamps must be monotonic within a single client clock")
        return self


class AttemptSample(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    request_id: str = Field(min_length=1)
    attempt_id: str = Field(min_length=1)
    worker_id: str = Field(min_length=1)
    attempt_number: int = Field(ge=1, le=2)
    reserved_cost: float = Field(ge=0)
    outcome: Literal["completed", "failed", "cancelled", "timeout"]
    gateway_request_id: str | None = None
    policy: str | None = None
    score: float | None = None
    affinity: float | None = Field(default=None, ge=0, le=1)
    seconds: float | None = Field(default=None, ge=0)
