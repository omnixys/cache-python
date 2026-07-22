from __future__ import annotations

from typing import Any

from redis.asyncio import Redis

from cache.model import CacheKeyDefinition, CacheSerializer
from cache.serializer import JsonCacheSerializer


class CacheClient:
    def __init__(
        self,
        url: str,
        key_prefix: str = "",
        serializer: CacheSerializer | None = None,
    ) -> None:
        self._client = Redis.from_url(
            url,
            decode_responses=True,
            health_check_interval=30,
        )
        self._prefix = key_prefix
        self._serializer = serializer or JsonCacheSerializer()

    async def ping(self) -> bool:
        return bool(await self._client.ping())

    async def close(self) -> None:
        await self._client.aclose()

    async def get(self, key: str | CacheKeyDefinition, *parts: str) -> str | None:
        key_str = self._resolve(key, *parts)
        value = await self._client.get(key_str)
        return str(value) if value is not None else None

    async def set(
        self,
        key: str | CacheKeyDefinition,
        value: str,
        ttl_seconds: int | None = None,
        *parts: str,
    ) -> None:
        key_str = self._resolve(key, *parts)
        ttl = self._resolve_ttl(key, ttl_seconds)
        await self._client.set(key_str, value, ex=ttl)

    async def delete(self, key: str | CacheKeyDefinition, *parts: str) -> int:
        key_str = self._resolve(key, *parts)
        result = await self._client.delete(key_str)
        return int(result)

    async def exists(self, key: str | CacheKeyDefinition, *parts: str) -> bool:
        key_str = self._resolve(key, *parts)
        return bool(await self._client.exists(key_str))

    async def expire(self, key: str | CacheKeyDefinition, ttl_seconds: int, *parts: str) -> bool:
        key_str = self._resolve(key, *parts)
        return bool(await self._client.expire(key_str, ttl_seconds))

    async def ttl(self, key: str | CacheKeyDefinition, *parts: str) -> int:
        key_str = self._resolve(key, *parts)
        result = await self._client.ttl(key_str)
        return int(result)

    async def increment(self, key: str | CacheKeyDefinition, ttl_seconds: int | None = None, *parts: str) -> int:
        key_str = self._resolve(key, *parts)
        val = await self._client.incr(key_str)
        ttl = self._resolve_ttl(key, ttl_seconds)
        if ttl is not None:
            await self._client.expire(key_str, ttl)
        return int(val)

    async def get_json(
        self,
        key: str | CacheKeyDefinition,
        *parts: str,
    ) -> Any | None:
        value = await self.get(key, *parts)
        if value is None:
            return None
        return self._serializer.deserialize(value)

    async def set_json(
        self,
        key: str | CacheKeyDefinition,
        value: Any,
        ttl_seconds: int | None = None,
        *parts: str,
    ) -> None:
        serialized = self._serializer.serialize(value)
        key_str = self._resolve(key, *parts)
        ttl = self._resolve_ttl(key, ttl_seconds)
        await self._client.set(key_str, serialized, ex=ttl)

    async def set_if_not_exists(
        self,
        key: str | CacheKeyDefinition,
        value: str,
        ttl_seconds: int | None = None,
        *parts: str,
    ) -> bool:
        key_str = self._resolve(key, *parts)
        ttl = self._resolve_ttl(key, ttl_seconds)
        result = await self._client.set(key_str, value, ex=ttl, nx=True)
        return result is not None

    def _resolve(self, key: str | CacheKeyDefinition, *parts: str) -> str:
        raw = key.key(*parts) if isinstance(key, CacheKeyDefinition) else ":".join((key, *parts)) if parts else key
        return f"{self._prefix}:{raw}" if self._prefix else raw

    @staticmethod
    def _resolve_ttl(key: str | CacheKeyDefinition, ttl_seconds: int | None) -> int | None:
        if ttl_seconds is not None:
            return ttl_seconds
        if isinstance(key, CacheKeyDefinition):
            return key.ttl_seconds
        return None
