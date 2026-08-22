"""Redis Streams consumer-group support for reliable work queues."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from cache.client import CacheClient

_DATA_FIELD = "data"


@dataclass(frozen=True, slots=True)
class StreamMessage[T]:
    id: str
    data: T


class ValkeyStreamService:
    """Stream + consumer-group facade over a shared CacheClient.

    Messages are stored as JSON in a single `data` field so payloads stay
    transport-neutral.
    """

    def __init__(self, client: CacheClient) -> None:
        self._client = client

    @property
    def raw(self) -> Redis:
        return self._client.raw

    async def ensure_group(self, stream: str, group: str) -> None:
        """Create `group` on `stream` if missing (idempotent)."""
        try:
            await self.raw.xgroup_create(stream, group, id="$", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" in str(exc):
                return
            raise

    async def enqueue(self, stream: str, payload: Any) -> str:
        """Append `payload` to `stream`; returns the message id."""
        result = await self.raw.xadd(stream, {_DATA_FIELD: json.dumps(payload)})
        return str(result)

    async def consume(
        self,
        stream: str,
        group: str,
        consumer: str,
        count: int = 10,
        block_ms: int = 5000,
    ) -> list[StreamMessage[Any]]:
        """Read new (unconsumed) messages from `group` as `consumer`."""
        response = await self.raw.xreadgroup(
            group,
            consumer,
            {stream: ">"},
            count=count,
            block=block_ms,
        )
        return self._flatten(response)

    async def read_pending(
        self,
        stream: str,
        group: str,
        consumer: str,
        count: int = 10,
    ) -> list[StreamMessage[Any]]:
        """Read already-delivered but unacknowledged messages (id '0')."""
        response = await self.raw.xreadgroup(
            group,
            consumer,
            {stream: "0"},
            count=count,
        )
        return self._flatten(response)

    async def ack(self, stream: str, group: str, message_id: str) -> None:
        await self.raw.xack(stream, group, message_id)

    def _flatten(self, response: Any) -> list[StreamMessage[Any]]:
        if not response:
            return []
        messages: list[StreamMessage[Any]] = []
        for entry in response:
            _, entries = entry
            for message_id, fields in entries:
                payload = fields.get(_DATA_FIELD.encode(), fields.get(_DATA_FIELD))
                data: Any = None
                if payload is not None:
                    decoded = payload.decode("utf-8") if isinstance(payload, bytes) else payload
                    data = json.loads(decoded)
                messages.append(StreamMessage(str(message_id), data))
        return messages
