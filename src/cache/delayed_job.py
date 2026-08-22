"""Delayed jobs scheduled on a Redis sorted set with persisted records.

A job is scheduled with an absolute execution timestamp. `DelayedJobWorker`
atomically claims due jobs (ZRANGEBYSCORE + ZREM via a WATCH/MULTI
transaction) and executes the registered handler through the
`DelayedJobRegistry`. Failed jobs are retried with a linear backoff until
`max_retries` is exhausted, after which they are marked as failed.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import secrets
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal

import redis as redis_pkg
from redis.asyncio import Redis

from cache.client import CacheClient

QUEUE_KEY = "delayed:jobs:scheduled"
RECORD_KEY_PREFIX = "delayed:job"
RECORD_TTL_SECONDS = 7 * 24 * 60 * 60

DelayedJobState = Literal["scheduled", "running", "completed", "failed", "canceled"]

JobHandler = Callable[[dict[str, Any]], Awaitable[Any]]


def now_ms() -> int:
    return int(datetime.now(UTC).timestamp() * 1000)


@dataclass(frozen=True, slots=True)
class DelayedJobContext:
    request_id: str = "unscoped"
    correlation_id: str = "unscoped"
    trace_id: str | None = None
    actor_id: str | None = None
    tenant_id: str | None = None


@dataclass(slots=True)
class DelayedJob:
    id: str
    type: str
    payload: dict[str, Any]
    execute_at_ms: int
    retries: int = 0
    max_retries: int = 3
    retry_delay_ms: int = 1000
    status: DelayedJobState = "scheduled"
    created_at_ms: int = field(default_factory=now_ms)
    updated_at_ms: int = field(default_factory=now_ms)
    last_error: str | None = None
    context: DelayedJobContext | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "type": self.type,
            "payload": self.payload,
            "executeAtMs": self.execute_at_ms,
            "retries": self.retries,
            "maxRetries": self.max_retries,
            "retryDelayMs": self.retry_delay_ms,
            "status": self.status,
            "createdAtMs": self.created_at_ms,
            "updatedAtMs": self.updated_at_ms,
            "lastError": self.last_error,
            "context": dataclass_to_dict(self.context),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DelayedJob:
        return cls(
            id=str(data["id"]),
            type=str(data["type"]),
            payload=data.get("payload") or {},
            execute_at_ms=int(data["executeAtMs"]),
            retries=int(data.get("retries", 0)),
            max_retries=int(data.get("maxRetries", 3)),
            retry_delay_ms=int(data.get("retryDelayMs", 1000)),
            status=data.get("status", "scheduled"),
            created_at_ms=int(data.get("createdAtMs", 0)),
            updated_at_ms=int(data.get("updatedAtMs", 0)),
            last_error=data.get("lastError"),
            context=_context_from_dict(data.get("context")),
        )


def dataclass_to_dict(context: DelayedJobContext | None) -> dict[str, str | None] | None:
    if context is None:
        return None
    return {
        "requestId": context.request_id,
        "correlationId": context.correlation_id,
        "traceId": context.trace_id,
        "actorId": context.actor_id,
        "tenantId": context.tenant_id,
    }


def _context_from_dict(data: Any) -> DelayedJobContext | None:
    if not isinstance(data, dict):
        return None
    return DelayedJobContext(
        request_id=str(data.get("requestId", "unscoped")),
        correlation_id=str(data.get("correlationId", "unscoped")),
        trace_id=data.get("traceId"),
        actor_id=data.get("actorId"),
        tenant_id=data.get("tenantId"),
    )


@dataclass(frozen=True, slots=True)
class DelayedJobSchedule:
    type: str
    payload: dict[str, Any]
    delay_ms: int
    max_retries: int = 3
    retry_delay_ms: int = 1000
    context: DelayedJobContext | None = None


class DelayedJobHandlerNotFoundError(Exception):
    def __init__(self, job_type: str) -> None:
        self.job_type = job_type
        super().__init__(f"No handler registered for delayed job type '{job_type}'")


class DelayedJobRegistry:
    """Maps delayed-job types to their async handlers."""

    def __init__(self) -> None:
        self._handlers: dict[str, JobHandler] = {}

    def register(self, job_type: str, handler: JobHandler) -> None:
        self._handlers[job_type] = handler

    def execute(self, job_type: str, payload: dict[str, Any]) -> Awaitable[Any]:
        handler = self._handlers.get(job_type)
        if handler is None:
            raise DelayedJobHandlerNotFoundError(job_type)
        return handler(payload)

    def has(self, job_type: str) -> bool:
        return job_type in self._handlers

    def handlers(self) -> tuple[str, ...]:
        return tuple(sorted(self._handlers))

    def __len__(self) -> int:
        return len(self._handlers)


_default_registry = DelayedJobRegistry()


def get_default_registry() -> DelayedJobRegistry:
    return _default_registry


def delayed_job(job_type: str) -> Callable[[JobHandler], JobHandler]:
    """Decorator that registers an async handler for a delayed-job type."""

    def decorator(handler: JobHandler) -> JobHandler:
        _default_registry.register(job_type, handler)
        return handler

    return decorator


class DelayedJobService:
    def __init__(self, client: CacheClient) -> None:
        self._client = client

    @property
    def raw(self) -> Redis:
        return self._client.raw

    async def schedule(self, schedule: DelayedJobSchedule) -> str:
        """Schedule a job and return its id."""
        if schedule.delay_ms < 0:
            raise ValueError("Delayed job delay_ms must be non-negative")
        if schedule.max_retries < 0:
            raise ValueError("Delayed job max_retries must be non-negative")

        now = now_ms()
        job = DelayedJob(
            id=secrets.token_hex(16),
            type=schedule.type,
            payload=schedule.payload,
            execute_at_ms=now + schedule.delay_ms,
            max_retries=schedule.max_retries,
            retry_delay_ms=schedule.retry_delay_ms,
            context=schedule.context,
        )
        await self.persist(job)
        await self.raw.zadd(QUEUE_KEY, {job.id: job.execute_at_ms})
        return job.id

    async def cancel(self, job_id: str) -> bool:
        job = await self.status(job_id)
        if job is None or job.status in ("completed", "running", "canceled", "failed"):
            return False
        await self.raw.zrem(QUEUE_KEY, job_id)
        job.status = "canceled"
        job.updated_at_ms = now_ms()
        await self.persist(job)
        return True

    async def retry(self, job_id: str, delay_ms: int = 0) -> bool:
        if delay_ms < 0:
            raise ValueError("Delayed job retry delay must be non-negative")
        job = await self.status(job_id)
        if job is None or job.status == "running":
            return False
        job.status = "scheduled"
        job.execute_at_ms = now_ms() + delay_ms
        job.updated_at_ms = now_ms()
        job.last_error = None
        await self.persist(job)
        await self.raw.zadd(QUEUE_KEY, {job.id: job.execute_at_ms})
        return True

    async def status(self, job_id: str) -> DelayedJob | None:
        raw = await self.raw.get(self._record_key(job_id))
        if not raw:
            return None
        try:
            return DelayedJob.from_dict(json.loads(raw))
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            return None

    async def claim_due(self, count: int = 10) -> list[DelayedJob]:
        """Atomically claim up to `count` due jobs and mark them running."""
        claimed_ids = await self._claim_due_ids(count)
        jobs: list[DelayedJob] = []
        for raw_id in claimed_ids:
            job = await self.status(str(raw_id))
            if job is None or job.status != "scheduled":
                continue
            job.status = "running"
            job.updated_at_ms = now_ms()
            await self.persist(job)
            jobs.append(job)
        return jobs

    async def _claim_due_ids(self, count: int) -> list[str]:
        pipe = self.raw.pipeline(transaction=True)
        while True:
            try:
                await pipe.watch(QUEUE_KEY)
                ids = await pipe.zrangebyscore(QUEUE_KEY, "-inf", now_ms(), start=0, num=count)
                if not ids:
                    await pipe.unwatch()  # type: ignore[no-untyped-call]
                    return []
                pipe.multi()  # type: ignore[no-untyped-call]
                pipe.zrem(QUEUE_KEY, *ids)
                await pipe.execute()
                return [str(item) for item in ids]
            except redis_pkg.WatchError:
                continue

    async def complete(self, job: DelayedJob) -> None:
        job.status = "completed"
        job.updated_at_ms = now_ms()
        await self.persist(job)

    async def fail(self, job: DelayedJob, error: Any) -> None:
        job.retries += 1
        job.last_error = str(error) if error else "unknown error"
        job.updated_at_ms = now_ms()
        if job.retries <= job.max_retries:
            job.status = "scheduled"
            job.execute_at_ms = now_ms() + job.retry_delay_ms * job.retries
            await self.persist(job)
            await self.raw.zadd(QUEUE_KEY, {job.id: job.execute_at_ms})
            return
        job.status = "failed"
        await self.persist(job)

    async def persist(self, job: DelayedJob) -> None:
        await self.raw.set(
            self._record_key(job.id),
            json.dumps(job.to_dict()),
            ex=RECORD_TTL_SECONDS,
        )

    def _record_key(self, job_id: str) -> str:
        return f"{RECORD_KEY_PREFIX}:{job_id}"


class DelayedJobWorker:
    """Background loop that claims and executes due delayed jobs."""

    def __init__(
        self,
        service: DelayedJobService,
        registry: DelayedJobRegistry | None = None,
        *,
        claim_count: int = 10,
        poll_interval_ms: int = 250,
        on_error: Callable[[str, Any], None] | None = None,
    ) -> None:
        self._service = service
        self._registry = registry or _default_registry
        self._claim_count = claim_count
        self._poll_interval_ms = poll_interval_ms
        self._on_error = on_error
        self._running = False
        self._in_flight = 0
        self._task: asyncio.Task[None] | None = None

    def ready(self) -> bool:
        return self._running

    def status(self) -> DelayedJobState:
        if not self._running:
            return "running" if self._in_flight > 0 else "canceled"
        return "running" if self._in_flight > 0 else "scheduled"

    def health(self) -> dict[str, Any]:
        return {"healthy": self._running, "status": self.status()}

    def diagnostics(self) -> dict[str, Any]:
        return {
            "status": self.status(),
            "inFlight": self._in_flight,
            "handlers": self._registry.handlers(),
        }

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run(), name="delayed-job-worker")

    async def _run(self) -> None:
        while self._running:
            try:
                jobs = await self._service.claim_due(self._claim_count)
                for job in jobs:
                    await self._handle(job)
                if not jobs:
                    await asyncio.sleep(self._poll_interval_ms / 1000)
            except asyncio.CancelledError:
                break
            except Exception as exc:  # noqa: BLE001
                if not self._running:
                    break
                if self._on_error is not None:
                    self._on_error("delayed_job_worker", exc)
                await asyncio.sleep(1.0)

    async def _handle(self, job: DelayedJob) -> None:
        self._in_flight += 1
        try:
            await self._registry.execute(job.type, job.payload)
            await self._service.complete(job)
        except Exception as exc:  # noqa: BLE001
            await self._service.fail(job, exc)
            if self._on_error is not None:
                self._on_error(job.type, exc)
        finally:
            self._in_flight -= 1

    async def drain(self, timeout_ms: int = 5000) -> None:
        deadline = now_ms() + timeout_ms
        while self._in_flight > 0:
            if now_ms() >= deadline:
                raise TimeoutError(f"Delayed job drain timed out after {timeout_ms}ms")
            await asyncio.sleep(0.01)

    async def close(self) -> None:
        if self._task is None and not self._running:
            return
        self._running = False
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        await self.drain()

    shutdown = close
