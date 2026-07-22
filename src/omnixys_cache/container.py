from __future__ import annotations

from dishka import Provider, Scope, provide
from redis.asyncio import Redis

from cache.client import CacheClient
from cache.invalidation import CacheInvalidationService
from cache.serializer import JsonCacheSerializer


class CacheProvider(Provider):
    scope = Scope.APP

    @provide
    def serializer(self) -> JsonCacheSerializer:
        return JsonCacheSerializer()

    @provide
    def cache_client(
        self,
        url: str,
        key_prefix: str = "",
        serializer: JsonCacheSerializer | None = None,
    ) -> CacheClient:
        return CacheClient(url=url, key_prefix=key_prefix, serializer=serializer)

    @provide
    def invalidation(self, url: str, channel: str = "omnixys:cache:invalidate") -> CacheInvalidationService:
        return CacheInvalidationService(
            redis=Redis.from_url(url, decode_responses=True),
            channel=channel,
        )
