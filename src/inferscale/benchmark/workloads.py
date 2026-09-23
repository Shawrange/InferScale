import math
import random
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Item(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    offset: float = Field(ge=0)
    group: str
    prompt: str = Field(min_length=1, max_length=100000)
    max_tokens: int = Field(gt=0, le=512)


class Trace(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    schema_version: Literal[1] = 1
    workload: Literal["mixed", "shared_prefix", "hot_prefix"]
    seed: int
    duration: float = Field(gt=0, le=86400)
    rate: float = Field(gt=0)
    warmup: tuple[Item, ...] = Field(max_length=100)
    requests: tuple[Item, ...] = Field(min_length=1, max_length=20000)

    @model_validator(mode="after")
    def ordered(self):
        offsets = [item.offset for item in self.requests]
        if offsets != sorted(offsets) or offsets[-1] >= self.duration:
            raise ValueError("arrivals must be ordered and inside measurement window")
        return self


def generate(workload: str, seed: int, duration: float, rate: float) -> Trace:
    if not math.isfinite(duration * rate) or not 0 < duration * rate <= 20000:
        raise ValueError("duration*rate must be in (0, 20000]")
    rng = random.Random(seed)
    common = "Reference document. " + (
        "A replica serves independent requests. Capacity is bounded. " * 45
    )
    items = []
    for index in range(math.ceil(duration * rate)):
        tag = f"{rng.getrandbits(64):016x}"
        if workload == "mixed":
            long_prompt = index % 2 == 1
            long_output = (index // 2) % 2 == 1
            prefix = f"Document {tag}. " + (common if long_prompt else "A tree grows.")
            group = (
                f"{'long' if long_prompt else 'short'}_input_"
                f"{'long' if long_output else 'short'}_cap"
            )
            cap = 256 if long_output else 32
        elif workload == "shared_prefix":
            prefix, group, cap = common, "shared", 64
        elif workload == "hot_prefix":
            hot = rng.random() < 0.85
            prefix = common if hot else f"Independent document {tag}. " + common
            group, cap = ("hot", 128) if hot else ("background", 256)
        else:
            raise ValueError("unknown workload")
        prompt = prefix + f"\nQuestion {tag}: explain the document in detail using numbered points."
        items.append(Item(offset=index / rate, group=group, prompt=prompt, max_tokens=cap))
    # Fixed single seed request establishes initial prefix/affinity on the first
    # clean worker. All policies use exactly the same seed and arrival trace.
    warm = Item(offset=0, group="seed", prompt=common + "\nSummarize briefly.", max_tokens=16)
    return Trace(
        workload=workload,
        seed=seed,
        duration=duration,
        rate=rate,
        warmup=(warm,),
        requests=tuple(items),
    )
