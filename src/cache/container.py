from __future__ import annotations

from dishka import Provider, Scope, provide
from redis.asyncio import Redis

from cache.client import CacheClient
from cache.delayed_job import DelayedJobRegistry, DelayedJobService, get_default_registry
from cache.invalidation import CacheInvalidationService
from cache.lock import CacheLock
from cache.rate_limit import RateLimiter
from cache.serializer import JsonCacheSerializer
from cache.stream import ValkeyStreamService


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

    @provide
    def lock(self, client: CacheClient) -> CacheLock:
        return CacheLock(client)

    @provide
    def rate_limiter(self, client: CacheClient) -> RateLimiter:
        return RateLimiter(client)

    @provide
    def stream_service(self, client: CacheClient) -> ValkeyStreamService:
        return ValkeyStreamService(client)

    @provide
    def delayed_job_service(self, client: CacheClient) -> DelayedJobService:
        return DelayedJobService(client)

    @provide
    def delayed_job_registry(self) -> DelayedJobRegistry:
        return get_default_registry()
