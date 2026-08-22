"""Behavioral tests for the fixed-window rate limiter."""

from __future__ import annotations

import fakeredis
import pytest

from cache import CacheClient, RateLimiter


@pytest.fixture
def client() -> CacheClient:
    return CacheClient(client=fakeredis.FakeAsyncRedis(decode_responses=True))


@pytest.fixture
def limiter(client: CacheClient) -> RateLimiter:
    return RateLimiter(client)


async def test_hit_within_limit(limiter: RateLimiter) -> None:
    assert await limiter.hit("ip:1.2.3.4", limit=3, ttl_seconds=10) is True
    assert await limiter.hit("ip:1.2.3.4", limit=3, ttl_seconds=10) is True
    assert await limiter.hit("ip:1.2.3.4", limit=3, ttl_seconds=10) is True
    assert await limiter.hit("ip:1.2.3.4", limit=3, ttl_seconds=10) is False


async def test_hit_resets_after_window(client: CacheClient, limiter: RateLimiter) -> None:
    await limiter.hit("k", limit=1, ttl_seconds=1)
    assert await limiter.hit("k", limit=1, ttl_seconds=1) is False
    await client.raw.flushdb()
    assert await limiter.hit("k", limit=1, ttl_seconds=1) is True


async def test_keys_are_isolated(limiter: RateLimiter) -> None:
    await limiter.hit("a", limit=1, ttl_seconds=10)
    assert await limiter.hit("b", limit=1, ttl_seconds=10) is True


async def test_check_reports_remaining(limiter: RateLimiter) -> None:
    result = await limiter.check("k", limit=2, ttl_seconds=10)
    assert result.allowed is True
    assert result.current == 1
    assert result.limit == 2
    assert result.remaining == 1
    second = await limiter.check("k", limit=2, ttl_seconds=10)
    assert second.allowed is True
    assert second.current == 2
    assert second.remaining == 0
    blocked = await limiter.check("k", limit=2, ttl_seconds=10)
    assert blocked.allowed is False
    assert blocked.remaining == 0
