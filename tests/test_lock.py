"""Behavioral tests for the distributed lock."""

from __future__ import annotations

import fakeredis
import pytest

from cache import CacheClient, CacheLock, LockNotAcquiredError


@pytest.fixture
def client() -> CacheClient:
    return CacheClient(client=fakeredis.FakeAsyncRedis(decode_responses=True))


@pytest.fixture
def lock(client: CacheClient) -> CacheLock:
    return CacheLock(client)


async def test_acquire_returns_token(lock: CacheLock) -> None:
    token = await lock.acquire("seat:1")
    assert token
    assert await lock.is_locked("seat:1") is True


async def test_acquire_rejects_second_holder(lock: CacheLock) -> None:
    first = await lock.acquire("seat:1")
    assert first is not None
    assert await lock.acquire("seat:1") is None
    assert await lock.release("seat:1", first) is True
    assert await lock.acquire("seat:1") is not None


async def test_release_only_with_matching_token(lock: CacheLock) -> None:
    token = await lock.acquire("seat:1")
    assert token is not None
    assert await lock.release("seat:1", "wrong-token") is False
    assert await lock.is_locked("seat:1") is True
    assert await lock.release("seat:1", token) is True
    assert await lock.is_locked("seat:1") is False


async def test_lock_ttl_expiry(lock: CacheLock) -> None:
    token = await lock.acquire("short", ttl_ms=50)
    assert token is not None
    import asyncio

    await asyncio.sleep(0.1)
    assert await lock.acquire("short") is not None


async def test_locked_context_manager(lock: CacheLock, client: CacheClient) -> None:
    async with lock.locked("critical"):
        assert await client.exists("critical")
    assert await client.exists("critical") is False


async def test_locked_context_raises_when_held(lock: CacheLock) -> None:
    token = await lock.acquire("critical")
    assert token is not None
    with pytest.raises(LockNotAcquiredError):
        async with lock.locked("critical"):
            pass
