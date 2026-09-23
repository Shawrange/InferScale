import json
from collections.abc import Iterator
from dataclasses import dataclass

from inferscale.api import APIError


@dataclass(frozen=True)
class Event:
    wire: bytes
    data: str | None


class SSEParser:
    """Bounded incremental framing. Normalizes CR/CRLF/LF, preserves field order."""

    def __init__(self, limit: int):
        self.limit = limit
        self.line = bytearray()
        self.lines: list[bytes] = []
        self.size = 0
        self.after_cr = False

    def feed(self, chunk: bytes) -> Iterator[Event]:
        for byte in chunk:
            if self.after_cr:
                self.after_cr = False
                if byte == 10:
                    continue
            self.size += 1
            if self.size > self.limit:
                raise APIError(502, "upstream_event_too_large")
            if byte not in (10, 13):
                self.line.append(byte)
                continue
            self.after_cr = byte == 13
            line = bytes(self.line)
            self.line.clear()
            if line:
                self.lines.append(line)
                continue
            if self.lines:
                raw_lines, self.lines = self.lines, []
                try:
                    data_lines = []
                    for raw in raw_lines:
                        text = raw.decode("utf-8")
                        if text == "data":
                            data_lines.append("")
                        elif text.startswith("data:"):
                            value = text[5:]
                            data_lines.append(value[1:] if value.startswith(" ") else value)
                    event = Event(
                        b"\n".join(raw_lines) + b"\n\n",
                        "\n".join(data_lines) if data_lines else None,
                    )
                except UnicodeDecodeError as exc:
                    raise APIError(502, "invalid_upstream_utf8") from exc
                self.size = 0
                yield event
            else:
                self.size = 0


class StreamInspector:
    def __init__(self, chat: bool):
        self.chat = chat
        self.finished = False
        self.done = False
        self.response_id: str | None = None
        self.usage = None
        self.finish_reason = None

    def inspect(self, event: Event) -> bool:
        """Return True for a nonempty text delta. DONE requires a finish event."""
        if event.data is None:
            return False
        if event.data == "[DONE]":
            if not self.finished:
                raise APIError(502, "upstream_done_without_finish")
            self.done = True
            return False
        try:
            data = json.loads(event.data)
            if not isinstance(data, dict) or "error" in data:
                raise ValueError
            response_id = data.get("id")
            if not isinstance(response_id, str) or not response_id:
                raise ValueError
            if self.response_id is not None and response_id != self.response_id:
                raise ValueError
            self.response_id = response_id
            if data.get("usage") is not None:
                self.usage = data["usage"]
            choices = data["choices"]
            if not isinstance(choices, list) or len(choices) > 1:
                raise ValueError
            if not choices:
                if not isinstance(data.get("usage"), dict):
                    raise ValueError
                return False
            choice = choices[0]
            if choice.get("index") != 0:
                raise ValueError
            content = choice.get("delta", {}).get("content") if self.chat else choice.get("text")
            if content is not None and not isinstance(content, str):
                raise ValueError
            if content and self.finished:
                raise ValueError
            if choice.get("finish_reason") is not None:
                if choice["finish_reason"] not in {"stop", "length", "content_filter"}:
                    raise ValueError
                self.finished = True
                self.finish_reason = choice["finish_reason"]
            return bool(content)
        except (ValueError, KeyError, TypeError, AttributeError) as exc:
            raise APIError(502, "invalid_upstream_event") from exc
