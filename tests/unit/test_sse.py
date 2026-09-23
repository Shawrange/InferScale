import pytest

from inferscale.api import APIError
from inferscale.sse import SSEParser, StreamInspector
from inferscale.testing.worker import sse


@pytest.mark.parametrize("separator", [b"\n", b"\r", b"\r\n"])
@pytest.mark.parametrize("chunk_size", [1, 3, 10000])
def test_incremental_utf8_and_multiple_events(separator, chunk_size):
    wire = b": ping\n\n" + sse({"text": "世界"}) + sse("[DONE]")
    wire = wire.replace(b"\n", separator)
    parser = SSEParser(1024)
    events = []
    for offset in range(0, len(wire), chunk_size):
        events.extend(parser.feed(wire[offset : offset + chunk_size]))
    assert len(events) == 3
    assert events[0].data is None
    assert "世界" in events[1].data
    assert events[2].data == "[DONE]"


def test_long_line_is_bounded_even_without_delimiter():
    with pytest.raises(APIError, match="too_large"):
        list(SSEParser(32).feed(b"x" * 33))


def test_multiline_data_and_bad_utf8():
    assert list(SSEParser(100).feed(b"data: one\ndata: two\n\n"))[0].data == "one\ntwo"
    with pytest.raises(APIError, match="utf8"):
        list(SSEParser(100).feed(b"data: \xff\n\n"))


def test_role_usage_and_done_require_real_finish():
    def event(data):
        return list(SSEParser(1024).feed(sse(data)))[0]

    inspector = StreamInspector(chat=True)
    assert not inspector.inspect(
        event({"id": "a", "choices": [{"index": 0, "delta": {"role": "assistant"}}]})
    )
    assert not inspector.inspect(
        event({"id": "a", "choices": [], "usage": {"completion_tokens": 0}})
    )
    with pytest.raises(APIError):
        inspector.inspect(event("[DONE]"))
    assert not inspector.inspect(
        event({"id": "a", "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]})
    )
    assert not inspector.inspect(event("[DONE]"))
    assert inspector.done
