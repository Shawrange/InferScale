import json
import logging
from collections import Counter

logger = logging.getLogger("inferscale.requests")


def configure_logging():
    """CLI JSON request logs; avoid changing third-party/root logger verbosity."""
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


class Telemetry:
    def __init__(self):
        self.counts: Counter[str] = Counter()
        self.first_output_count = 0
        self.first_output_seconds = 0.0

    def increment(self, key: str):
        self.counts[key] += 1

    def first_output(self, seconds: float):
        self.first_output_count += 1
        self.first_output_seconds += seconds

    def log(self, **fields):
        # Call sites supply only IDs, timing, outcomes and error codes, never request payloads.
        logger.info(json.dumps(fields, ensure_ascii=False))

    def metrics(self) -> str:
        lines = []
        for name in (
            "retries",
            "partial_failures",
            "cleanup_failures",
            "cancelled",
            "timeouts",
            "rejected",
            "completed",
            "failed",
        ):
            lines.extend(
                (
                    f"# TYPE inferscale_{name}_total counter",
                    f"inferscale_{name}_total {self.counts[name]}",
                )
            )
        lines.extend(
            (
                "# TYPE inferscale_first_output_wait_seconds summary",
                f"inferscale_first_output_wait_seconds_count {self.first_output_count}",
                f"inferscale_first_output_wait_seconds_sum {self.first_output_seconds}",
            )
        )
        return "\n".join(lines) + "\n"
