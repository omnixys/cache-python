"""Behavioral tests for the delayed job service and worker."""

from __future__ import annotations

import asyncio

import fakeredis
import pytest

from cache import (
    CacheClient,
    DelayedJobContext,
    DelayedJobHandlerNotFoundError,
    DelayedJobRegistry,
    DelayedJobSchedule,
    DelayedJobService,
    DelayedJobWorker,
    delayed_job,
)


@pytest.fixture
def client() -> CacheClient:
    return CacheClient(client=fakeredis.FakeAsyncRedis(decode_responses=True))


@pytest.fixture
def service(client: CacheClient) -> DelayedJobService:
    return DelayedJobService(client)


async def test_schedule_and_status(service: DelayedJobService) -> None:
    job_id = await service.schedule(
        DelayedJobSchedule(type="email", payload={"to": "a@b.c"}, delay_ms=1000),
    )
    job = await service.status(job_id)
    assert job is not None
    assert job.id == job_id
    assert job.type == "email"
    assert job.payload == {"to": "a@b.c"}
    assert job.status == "scheduled"
    assert job.execute_at_ms > 0


async def test_schedule_validates(service: DelayedJobService) -> None:
    with pytest.raises(ValueError):
        await service.schedule(DelayedJobSchedule(type="x", payload={}, delay_ms=-1))
    with pytest.raises(ValueError):
        await service.schedule(DelayedJobSchedule(type="x", payload={}, delay_ms=0, max_retries=-1))


async def test_claim_due_only_claims_ready_jobs(service: DelayedJobService) -> None:
    future = await service.schedule(DelayedJobSchedule(type="a", payload={}, delay_ms=60_000))
    ready = await service.schedule(DelayedJobSchedule(type="b", payload={}, delay_ms=0))
    claimed = await service.claim_due()
    assert [job.id for job in claimed] == [ready]
    assert future not in [job.id for job in claimed]


async def test_claim_due_marks_running(service: DelayedJobService) -> None:
    job_id = await service.schedule(DelayedJobSchedule(type="b", payload={}, delay_ms=0))
    claimed = await service.claim_due()
    assert claimed[0].status == "running"
    assert (await service.status(job_id)) is not None


async def test_complete(service: DelayedJobService) -> None:
    job_id = await service.schedule(DelayedJobSchedule(type="b", payload={}, delay_ms=0))
    await service.claim_due()
    job = await service.status(job_id)
    assert job is not None
    await service.complete(job)
    assert (await service.status(job_id)).status == "completed"


async def test_fail_retries_then_marks_failed(service: DelayedJobService) -> None:
    job_id = await service.schedule(
        DelayedJobSchedule(type="b", payload={}, delay_ms=0, max_retries=1, retry_delay_ms=1),
    )
    job = await service.status(job_id)
    assert job is not None
    await service.fail(job, ValueError("boom"))
    retried = await service.status(job_id)
    assert retried.status == "scheduled"
    assert retried.retries == 1
    assert retried.last_error == "boom"
    await service.claim_due()
    job2 = await service.status(job_id)
    await service.fail(job2, ValueError("boom"))
    assert (await service.status(job_id)).status == "failed"
    assert (await service.status(job_id)).retries == 2


async def test_cancel(service: DelayedJobService) -> None:
    job_id = await service.schedule(DelayedJobSchedule(type="b", payload={}, delay_ms=60_000))
    assert await service.cancel(job_id) is True
    assert (await service.status(job_id)).status == "canceled"
    assert await service.cancel(job_id) is False


async def test_retry_reschedules(service: DelayedJobService) -> None:
    job_id = await service.schedule(DelayedJobSchedule(type="b", payload={}, delay_ms=60_000))
    assert await service.retry(job_id, delay_ms=1000) is True
    job = await service.status(job_id)
    assert job.status == "scheduled"
    assert job.last_error is None


async def test_status_unknown(service: DelayedJobService) -> None:
    assert await service.status("missing") is None


async def test_persist_roundtrip(service: DelayedJobService) -> None:
    job_id = await service.schedule(
        DelayedJobSchedule(
            type="b",
            payload={"x": 1},
            delay_ms=0,
            context=DelayedJobContext(request_id="req-1", tenant_id="t-1"),
        ),
    )
    job = await service.status(job_id)
    assert job is not None
    assert job.context.request_id == "req-1"
    assert job.context.tenant_id == "t-1"


async def test_registry_execute_and_decorator() -> None:
    calls: list[str] = []

    @delayed_job("greet")
    async def handle(payload: dict) -> None:
        calls.append(payload["name"])

    from cache.delayed_job import get_default_registry

    assert get_default_registry().has("greet") is True
    await get_default_registry().execute("greet", {"name": "ada"})
    assert calls == ["ada"]


async def test_registry_handler_not_found() -> None:
    registry = DelayedJobRegistry()
    with pytest.raises(DelayedJobHandlerNotFoundError):
        await registry.execute("missing", {})


async def test_worker_executes_due_jobs(client: CacheClient) -> None:
    service = DelayedJobService(client)
    registry = DelayedJobRegistry()
    executed: list[str] = []

    async def handler(payload: dict) -> None:
        executed.append(payload["value"])

    registry.register("job", handler)
    job_id = await service.schedule(DelayedJobSchedule(type="job", payload={"value": "x"}, delay_ms=0))
    worker = DelayedJobWorker(service, registry, poll_interval_ms=10)
    await worker.start()
    try:
        await _wait_for(lambda: len(executed) > 0)
    finally:
        await worker.close()
    assert executed == ["x"]
    assert (await service.status(job_id)).status == "completed"


async def test_worker_marks_failed_after_retries(client: CacheClient) -> None:
    service = DelayedJobService(client)
    registry = DelayedJobRegistry()

    async def failing(payload: dict) -> None:
        raise RuntimeError("nope")

    registry.register("flaky", failing)
    job_id = await service.schedule(
        DelayedJobSchedule(type="flaky", payload={}, delay_ms=0, max_retries=2, retry_delay_ms=1),
    )
    worker = DelayedJobWorker(service, registry, poll_interval_ms=10)
    await worker.start()
    try:
        await _wait_for_async(lambda: service.status(job_id), lambda job: job.status == "failed")
    finally:
        await worker.close()
    final = await service.status(job_id)
    assert final.status == "failed"
    assert final.retries == 3


async def test_worker_health_and_diagnostics(client: CacheClient) -> None:
    service = DelayedJobService(client)
    registry = DelayedJobRegistry()

    async def handler(payload: dict) -> None:
        pass

    registry.register("job", handler)
    worker = DelayedJobWorker(service, registry, poll_interval_ms=10)
    assert worker.ready() is False
    await worker.start()
    try:
        assert worker.ready() is True
        assert worker.health()["healthy"] is True
        assert worker.diagnostics()["handlers"] == ("job",)
    finally:
        await worker.close()
    assert worker.ready() is False


async def _wait_for(condition, timeout: float = 2.0) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        result = condition()
        if asyncio.iscoroutine(result):
            result = await result
        if result:
            return
        await asyncio.sleep(0.02)
    raise AssertionError("condition not met within timeout")


async def _wait_for_async(fetch, predicate, timeout: float = 2.0) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        value = await fetch()
        if predicate(value):
            return
        await asyncio.sleep(0.02)
    raise AssertionError("condition not met within timeout")
