from __future__ import annotations

import json
from typing import Any


class JsonCacheSerializer:
    def serialize(self, value: Any) -> str:
        return json.dumps(value, default=str)

    def deserialize(self, data: str) -> Any | None:
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            return None


class StrictJsonCacheSerializer:
    def serialize(self, value: Any) -> str:
        return json.dumps(value, default=str)

    def deserialize(self, data: str) -> Any:
        try:
            return json.loads(data)
        except json.JSONDecodeError as exc:
            msg = f"Failed to deserialize cache value: {exc}"
            raise ValueError(msg) from exc
