from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, StrictStr


class APIError(Exception):
    def __init__(self, status: int, code: str, *, retryable: bool = False):
        self.status = status
        self.code = code
        self.retryable = retryable
        super().__init__(code)


class ClientGone(Exception):
    pass


class TextMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")
    role: Literal["system", "user", "assistant"]
    content: StrictStr


class StreamOptions(BaseModel):
    model_config = ConfigDict(extra="forbid")
    include_usage: StrictBool = False


class GenerationInput(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    model: StrictStr
    stream: StrictBool = False
    max_tokens: StrictInt | None = Field(default=None, gt=0)
    n: StrictInt = Field(default=1, ge=1, le=1)
    temperature: float | None = Field(default=None, ge=0, le=2)
    top_p: float | None = Field(default=None, gt=0, le=1)
    stream_options: StreamOptions | None = None


class ChatInput(GenerationInput):
    messages: list[TextMessage] = Field(min_length=1)


class CompletionInput(GenerationInput):
    prompt: StrictStr
