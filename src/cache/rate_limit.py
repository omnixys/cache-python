"""Fixed-window rate limiting backed by atomic Redis INCR/EXPIRE."""

from __future__ import annotations

from dataclasses import dataclass

from cache.client import CacheClient


@dataclass(frozen=True, slots=True)
class RateLimitResult:
    allowed: bool
    current: int
    limit: int
    remaining: int


class RateLimiter:
    """Fixed-window rate limiter.

    Each `hit` atomically increments the counter for `key` and sets the TTL on
    the first increment of the window. The window is therefore reset after
    `ttl_seconds` pass.
    """

    def __init__(self, client: CacheClient) -> None:
        self._client = client

    async def hit(self, key: str, limit: int, ttl_seconds: int) -> bool:
        """Record one hit; returns True if still within `limit` for the window."""
        current = await self._client.increment(key, ttl_seconds=ttl_seconds)
        return current <= limit

    async def check(self, key: str, limit: int, ttl_seconds: int) -> RateLimitResult:
        """Record one hit and return the full rate-limit result."""
        current = await self._client.increment(key, ttl_seconds=ttl_seconds)
        return RateLimitResult(
            allowed=current <= limit,
            current=current,
            limit=limit,
            remaining=max(limit - current, 0),
        )
