"""Distributed lock support backed by a Redis/Valkey SET NX PX primitive."""

from __future__ import annotations

import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import redis as redis_pkg
from redis.asyncio import Redis

from cache.client import CacheClient


class LockNotAcquiredError(Exception):
    def __init__(self, key: str) -> None:
        self.key = key
        super().__init__(f"Could not acquire lock for key '{key}'")


class CacheLock:
    """Non-blocking distributed lock with a safety TTL and token-based release.

    The lock is released atomically (WATCH + GET + MULTI DEL) only when the
    stored token matches, so a lock acquired by another holder can never be
    released by us.
    """

    def __init__(self, client: CacheClient) -> None:
        self._client = client

    @property
    def raw(self) -> Redis:
        return self._client.raw

    async def acquire(self, key: str, ttl_ms: int = 3000) -> str | None:
        """Try to acquire `key`; returns the release token or None if already held."""
        token = secrets.token_hex(16)
        acquired = await self._client.set_nx_px(key, token, ttl_ms)
        return token if acquired else None

    async def release(self, key: str, token: str) -> bool:
        """Release `key` if and only if it is still held by `token`."""
        pipe = self.raw.pipeline(transaction=True)
        while True:
            try:
                await pipe.watch(key)
                stored = await pipe.get(key)
                if stored != token:
                    await pipe.unwatch()  # type: ignore[no-untyped-call]
                    return False
                pipe.multi()  # type: ignore[no-untyped-call]
                pipe.delete(key)
                await pipe.execute()
            except redis_pkg.WatchError:
                continue
            else:
                return True

    async def is_locked(self, key: str) -> bool:
        return await self._client.exists(key)

    @asynccontextmanager
    async def locked(self, key: str, ttl_ms: int = 3000) -> AsyncIterator[None]:
        """Context manager that acquires `key` and raises if it is already held."""
        token = await self.acquire(key, ttl_ms)
        if token is None:
            raise LockNotAcquiredError(key)
        try:
            yield
        finally:
            await self.release(key, token)
