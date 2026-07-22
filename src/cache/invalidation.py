from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from redis.asyncio import Redis


class CacheInvalidationService:
    def __init__(self, redis: Redis, channel: str = "omnixys:cache:invalidate") -> None:
        self._redis = redis
        self._channel = channel
        self._pubsub = redis.pubsub()

    async def publish(
        self,
        key: str,
        operation: str = "delete",
        source: str = "",
        request_id: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        event = {
            "eventId": str(uuid4()),
            "key": key,
            "operation": operation,
            "source": source,
            "occurredAtEpochMs": int(datetime.now(UTC).timestamp() * 1000),
            "requestId": request_id or "",
            "correlationId": correlation_id or "",
        }
        await self._redis.publish(self._channel, json.dumps(event))

    async def subscribe(self) -> None:
        await self._pubsub.subscribe(self._channel)

    async def unsubscribe(self) -> None:
        await self._pubsub.unsubscribe(self._channel)

    async def get_message(self, poll_timeout: float = 1.0) -> dict[str, Any] | None:
        raw = await self._pubsub.get_message(timeout=poll_timeout, ignore_subscribe_messages=True)
        if raw is None or raw["type"] != "message":
            return None
        data = raw.get("data", "")
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        return json.loads(data) if data else None

    async def close(self) -> None:
        await self._pubsub.close()
