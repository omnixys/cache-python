from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class CacheSerializer(Protocol):
    def serialize(self, value: Any) -> str: ...
    def deserialize(self, data: str) -> Any: ...


@dataclass(frozen=True)
class CacheKeyDefinition:
    prefix: str
    ttl_seconds: int | None = None

    def key(self, *parts: str) -> str:
        return ":".join((self.prefix, *parts)) if parts else self.prefix
