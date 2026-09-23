import asyncio
import hmac
import json
from contextlib import aclosing, suppress
from dataclasses import asdict
from time import monotonic
from uuid import UUID, uuid4

import httpx
from pydantic import ValidationError
from starlette.responses import Response

from inferscale.api import APIError, ChatInput, ClientGone, CompletionInput
from inferscale.features import fingerprint
from inferscale.models import CapacityExceeded, NoSchedulableWorker, Outcome, RequestContext
from inferscale.sse import SSEParser, StreamInspector


class ProxyResponse(Response):
    """Owns ASGI receive/send and all resources until the logical request ends.

    No StreamingResponse/background task split: response-start is explicitly
    controlled, and cleanup runs even when prefetch or downstream send fails.
    """

    def __init__(self, app, *, chat: bool):
        super().__init__()
        self.app = app
        self.settings = app.state.settings
        self.ledger = app.state.ledger
        self.registry = app.state.registry
        self.telemetry = app.state.telemetry
        self.chat = chat
        self.request_id = uuid4().hex
        self.client_request_id = None
        self.attempt_outcome = Outcome.FAILED
        self.attempt_started_at = None
        self.lease = None
        self.attempt = None
        self.upstream = None
        self.commit_started = False
        self.response_started = False
        self.body_bytes_sent = 0
        self.delivery_complete = False
        self.disconnected = False
        self.first_output_at = None
        self.first_forward_at = None
        self.outcome = Outcome.FAILED
        self.error_code = None
        self.work = None
        self.listener = None
        self.open_pending = False
        self.closing = None
        self.features = None
        self.successful_attempt = None
        self.usage = None
        self.finish_reason = None
        self.started_at = monotonic()
        self.deadline = self.started_at + self.settings.overall_timeout_seconds

    async def __call__(self, scope, receive, send):
        self.send = send
        try:
            async with asyncio.timeout_at(self.deadline):
                self._authorize(scope)
                try:
                    self.lease = self.ledger.acquire(
                        RequestContext(self.request_id, self.settings.model.name)
                    )
                except CapacityExceeded as exc:
                    raise APIError(503, "global_capacity_exceeded") from exc
                payload = await self._read_body(receive)
                self.listener = asyncio.create_task(self._listen(receive), name="proxy-disconnect")
                self.work = asyncio.create_task(self._prepare(payload), name="proxy-generation")
                done, _ = await asyncio.wait(
                    (self.work, self.listener), return_when=asyncio.FIRST_COMPLETED
                )
                if self.work in done or self.delivery_complete:
                    await self.work
                    self.outcome = Outcome.COMPLETED
                else:
                    await self.listener
                    raise ClientGone
        except ClientGone:
            self.disconnected = True
            self.outcome = Outcome.CANCELLED
        except asyncio.CancelledError:
            self.outcome = Outcome.COMPLETED if self.delivery_complete else Outcome.CANCELLED
            raise
        except (TimeoutError, httpx.TimeoutException):
            if self.delivery_complete:
                # Cleanup has its own bounded budget. Do not rewrite an already
                # fully delivered response as a timeout or send a second body.
                self.outcome = Outcome.COMPLETED
            else:
                self.outcome = Outcome.TIMEOUT
                self.error_code = "request_timeout"
                await self._error(504, self.error_code)
        except APIError as exc:
            self.outcome = (
                Outcome.REJECTED
                if exc.status < 500
                or exc.code
                in {"global_capacity_exceeded", "no_worker_capacity", "tokenizer_capacity_exceeded"}
                else Outcome.FAILED
            )
            self.error_code = exc.code
            await self._error(exc.status, exc.code)
        except (OSError, httpx.HTTPError):
            self.outcome = Outcome.FAILED
            self.error_code = "connection_error"
            await self._error(502, self.error_code)
        except Exception:
            self.outcome = Outcome.FAILED
            self.error_code = "internal_error"
            await self._error(500, self.error_code)
        finally:
            # Shield only the bounded resource finalizer, not request execution.
            cleanup = asyncio.create_task(self._finish(), name="proxy-finalize")
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                await asyncio.shield(cleanup)
                raise

    def _authorize(self, scope):
        headers = dict(scope.get("headers", []))
        correlation = headers.get(b"x-inferscale-client-id")
        if correlation is not None:
            try:
                if len(correlation) > 36:
                    raise ValueError
                self.client_request_id = UUID(correlation.decode("ascii")).hex
            except (ValueError, UnicodeError) as exc:
                raise APIError(400, "invalid_client_request_id") from exc
        expected = self.app.state.api_key
        if expected:
            supplied = headers.get(b"authorization", b"")
            if not hmac.compare_digest(supplied, f"Bearer {expected}".encode()):
                raise APIError(401, "invalid_api_key")

    async def _read_body(self, receive) -> bytes:
        data = bytearray()
        async with asyncio.timeout(self.settings.body_timeout_seconds):
            while True:
                message = await receive()
                if message["type"] == "http.disconnect":
                    raise ClientGone
                chunk = message.get("body", b"")
                if len(data) + len(chunk) > self.settings.max_body_bytes:
                    raise APIError(413, "request_body_too_large")
                data.extend(chunk)
                if not message.get("more_body", False):
                    return bytes(data)

    async def _prepare(self, payload):
        body, cost = await self._validate(payload)
        await self._generate(body, cost)

    async def _validate(self, payload: bytes) -> tuple[dict, float]:
        try:
            schema = ChatInput if self.chat else CompletionInput
            request = schema.model_validate_json(payload)
        except ValidationError as exc:
            raise APIError(400, "invalid_request") from exc
        if request.model != self.settings.model.name:
            raise APIError(400, "unknown_model")
        body = request.model_dump(exclude_none=True)
        cap = request.max_tokens or self.settings.model.default_output_tokens
        if cap > self.settings.max_output_tokens:
            raise APIError(400, "output_limit_exceeded")
        body["max_tokens"] = cap
        if self.app.state.token_counter is not None:
            # Legacy synchronous count-only injection is for small CPU fixtures.
            prompt_tokens = self.app.state.token_counter(body, self.chat)
            prefix = None
        else:
            ids = await self.app.state.tokenizer.encode(body, self.chat)
            prompt_tokens = len(ids)
            prefix = fingerprint(self.settings.model, ids, self.settings.routing.prefix_tokens)
        if type(prompt_tokens) is not int or prompt_tokens < 0:
            raise APIError(400, "invalid_token_count")
        if prompt_tokens + cap > self.settings.model.context_limit:
            raise APIError(400, "context_limit_exceeded")
        with self.registry.lock:
            self.features = self.app.state.predictor.estimate(prompt_tokens, cap, prefix)
        return body, self.features.cost

    async def _listen(self, receive):
        # Only called after the full body has been read. There is one receive owner.
        while True:
            if (await receive())["type"] == "http.disconnect":
                self.disconnected = True
                return

    async def _generate(self, body: dict, cost: int):
        last_error = None
        for number in range(2):
            iterator = None
            try:
                self.attempt = self.ledger.select_and_reserve(
                    self.lease, cost, self.features.prefix
                )
            except NoSchedulableWorker as exc:
                raise last_error or APIError(503, "no_worker_capacity") from exc
            if number:
                self.telemetry.increment("retries")
            self.attempt_outcome = Outcome.FAILED
            self.attempt_started_at = monotonic()
            worker = next(
                w for w in self.registry.snapshots() if w.worker_id == self.attempt.worker_id
            )
            self.telemetry.log(
                event="routing_decision",
                request_id=self.request_id,
                client_request_id=self.client_request_id,
                attempt_id=self.attempt.attempt_id,
                attempt_number=self.attempt.attempt_number,
                worker_id=worker.worker_id,
                policy=self.attempt.policy,
                score=self.attempt.score,
                affinity=self.attempt.affinity,
                fallback_reason=self.attempt.fallback_reason,
                prompt_tokens=self.features.prompt_tokens,
                output_estimate=self.features.output_estimate,
                reserved_cost=self.attempt.cost,
                decision=asdict(self.attempt.decision) if self.attempt.decision else None,
            )
            try:
                if self.first_forward_at is None:
                    self.first_forward_at = monotonic()
                if body["stream"]:
                    # Includes connect, pool wait and role-only prelude; never reset on retry.
                    async with asyncio.timeout_at(
                        self.first_forward_at + self.settings.first_output_timeout_seconds
                    ):
                        await self._open(worker.endpoint, body)
                        iterator, prefetched, inspector = await self._prefetch()
                    await self._start(200, b"text/event-stream")
                    for event in prefetched:
                        await self._body(event.wire)
                    await self._relay(iterator, inspector)
                    self.usage, self.finish_reason = inspector.usage, inspector.finish_reason
                else:
                    await self._open(worker.endpoint, body)
                    raw = await self._json_response()
                    await self._start(200, b"application/json")
                    await self._body(raw, more=False)
                self.successful_attempt = self.attempt
                self.attempt_outcome = Outcome.COMPLETED
                return
            except asyncio.CancelledError:
                self.attempt_outcome = (
                    Outcome.TIMEOUT if self.outcome is Outcome.TIMEOUT else Outcome.CANCELLED
                )
                raise
            except httpx.PoolTimeout as exc:
                raise APIError(503, "upstream_pool_timeout") from exc
            except (TimeoutError, httpx.TimeoutException):
                self.attempt_outcome = Outcome.TIMEOUT
                raise
            except (
                httpx.ConnectError,
                httpx.ReadError,
                httpx.WriteError,
                httpx.RemoteProtocolError,
            ) as exc:
                last_error = APIError(502, "upstream_connection_failed", retryable=True)
                if number or self.commit_started or self.disconnected:
                    raise last_error from exc
            except APIError as exc:
                if number or not exc.retryable or self.commit_started or self.disconnected:
                    raise
                last_error = exc
            finally:
                if iterator is not None:
                    await iterator.aclose()
                await self._close_attempt()
            if monotonic() >= self.deadline or self.disconnected:
                raise last_error

    async def _open(self, endpoint: str, body: dict):
        path = "/v1/chat/completions" if self.chat else "/v1/completions"
        client = self.app.state.inference_client
        request = client.build_request(
            "POST",
            endpoint + path,
            json=body,
            headers={
                "x-request-id": self.request_id,
                "x-attempt-id": self.attempt.attempt_id,
                "accept-encoding": "identity",
            },
        )
        self.open_pending = True
        self.upstream = await client.send(request, stream=True)
        self.open_pending = False
        status = self.upstream.status_code
        if status != 200:
            code = status if status in {400, 429} else 502
            raise APIError(code, "upstream_rejected", retryable=status in {502, 503, 504})

    async def _events(self):
        if "text/event-stream" not in self.upstream.headers.get("content-type", ""):
            raise APIError(502, "invalid_upstream_content_type")
        parser = SSEParser(self.settings.max_event_bytes)
        async with aclosing(self.upstream.aiter_bytes()) as chunks:
            async for chunk in chunks:
                for event in parser.feed(chunk):
                    yield event
        raise APIError(502, "upstream_stream_truncated", retryable=True)

    async def _prefetch(self):
        iterator = self._events()
        inspector = StreamInspector(self.chat)
        pending, size = [], 0
        async for event in iterator:
            size += len(event.wire)
            if size > self.settings.max_prefetch_bytes:
                raise APIError(502, "upstream_prefetch_too_large")
            content = inspector.inspect(event)
            pending.append(event)
            if content:
                self.first_output_at = monotonic()
                self.telemetry.first_output(self.first_output_at - self.first_forward_at)
            if content or inspector.done:
                return iterator, pending, inspector
        raise APIError(502, "upstream_stream_truncated", retryable=True)

    async def _relay(self, iterator, inspector):
        idle_deadline = monotonic() + self.settings.idle_timeout_seconds
        while not inspector.done:
            async with asyncio.timeout_at(idle_deadline):
                event = await anext(iterator)
            content = inspector.inspect(event)
            if content:
                idle_deadline = monotonic() + self.settings.idle_timeout_seconds
            await self._body(event.wire)
        await self._body(b"", more=False)

    async def _json_response(self) -> bytes:
        data = bytearray()
        async for chunk in self.upstream.aiter_bytes():
            if len(data) + len(chunk) > self.settings.max_response_bytes:
                raise APIError(502, "upstream_response_too_large")
            data.extend(chunk)
        try:
            parsed = json.loads(data)
            choices = parsed["choices"]
            if len(choices) != 1 or not choices[0].get("finish_reason"):
                raise ValueError
            content = choices[0]["message"]["content"] if self.chat else choices[0]["text"]
            if not isinstance(content, str):
                raise ValueError
            self.usage = parsed.get("usage")
            self.finish_reason = choices[0]["finish_reason"]
        except (ValueError, KeyError, TypeError, IndexError, AttributeError) as exc:
            raise APIError(502, "invalid_upstream_json") from exc
        return bytes(data)

    async def _send(self, message):
        if self.disconnected:
            raise ClientGone
        async with asyncio.timeout(self.settings.downstream_timeout_seconds):
            await self.send(message)

    async def _start(self, status: int, content_type: bytes):
        self.commit_started = True  # BEFORE send: failed send has an uncertain delivery boundary.
        await self._send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [
                    (b"content-type", content_type),
                    (b"cache-control", b"no-cache"),
                    (b"x-inferscale-request-id", self.request_id.encode()),
                ],
            }
        )
        self.response_started = True

    async def _body(self, data: bytes, *, more=True):
        await self._send({"type": "http.response.body", "body": data, "more_body": more})
        self.body_bytes_sent += len(data)
        if not more:
            self.delivery_complete = True

    async def _error(self, status, code):
        # The generation task must not still be able to send response-start while
        # the outer timeout path is sending an HTTP error.
        if self.work and not self.work.done():
            self.work.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await self.work
        if self.disconnected or self.delivery_complete:
            return
        try:
            if not self.commit_started:
                await self._start(status, b"application/json")
                await self._body(
                    json.dumps({"error": {"code": code, "message": code}}).encode(), more=False
                )
            elif self.response_started:
                self.telemetry.increment("partial_failures")
                await self._body(b"", more=False)  # No successful SSE terminator.
        except (OSError, TimeoutError, ClientGone):
            pass

    async def _close_attempt(self):
        response, attempt = self.upstream, self.attempt
        self.upstream, self.attempt = None, None
        if response is None and attempt and self.open_pending:
            self.registry.quarantine(attempt.worker_id, attempt.worker_epoch)
        self.open_pending = False
        if response is None and attempt is None:
            return
        self.closing = asyncio.create_task(self._close_owned(response, attempt), name="proxy-close")
        try:
            await asyncio.shield(self.closing)
        except asyncio.CancelledError:
            await asyncio.shield(self.closing)
            raise

    async def _close_owned(self, response, attempt):
        try:
            if response is not None:
                async with asyncio.timeout(self.settings.cleanup_timeout_seconds):
                    await response.aclose()
        except Exception:
            self.telemetry.increment("cleanup_failures")
            if attempt:
                self.registry.quarantine(attempt.worker_id, attempt.worker_epoch)
        finally:
            if attempt:
                self.ledger.release_attempt(attempt)
                self.telemetry.log(
                    event="attempt_finished",
                    request_id=self.request_id,
                    client_request_id=self.client_request_id,
                    attempt_id=attempt.attempt_id,
                    attempt_number=attempt.attempt_number,
                    worker_id=attempt.worker_id,
                    outcome=self.attempt_outcome.value,
                    reserved_cost=attempt.cost,
                    policy=attempt.policy,
                    score=attempt.score,
                    affinity=attempt.affinity,
                    decision=asdict(attempt.decision) if attempt.decision else None,
                    seconds=(monotonic() - self.attempt_started_at)
                    if self.attempt_started_at is not None
                    else None,
                )

    async def _finish(self):
        try:
            for task in (self.work, self.listener):
                if task:
                    if not task.done():
                        task.cancel()
                    with suppress(asyncio.CancelledError, Exception):
                        await task
            await self._close_attempt()
            if self.closing:
                await self.closing
        finally:
            if self.lease:
                self.ledger.finish(self.lease, self.outcome)
            if self.outcome is Outcome.COMPLETED and self.successful_attempt and self.features:
                with self.registry.lock:
                    updated = self.app.state.predictor.observe(
                        self.features,
                        self.usage,
                        self.finish_reason,
                    )
                    self.ledger.remember_prefix(self.successful_attempt, self.features.prefix)
                self.telemetry.log(
                    event="prediction_observation",
                    request_id=self.request_id,
                    updated=updated,
                    finish_reason=self.finish_reason,
                    length_capped=updated
                    and (
                        self.finish_reason == "length"
                        or self.usage["completion_tokens"] == self.features.output_cap
                    ),
                )
            key = {Outcome.TIMEOUT: "timeouts"}.get(self.outcome, self.outcome.value)
            self.telemetry.increment(key)
            self.telemetry.log(
                event="request_finished",
                request_id=self.request_id,
                client_request_id=self.client_request_id,
                outcome=self.outcome.value,
                code=self.error_code,
                seconds=monotonic() - self.started_at,
                first_output_at=self.first_output_at,
                response_started=self.response_started,
                bytes_sent=self.body_bytes_sent,
            )
