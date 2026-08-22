"""Typed cache key definitions and the `create_key` builder."""

from __future__ import annotations

import math

from cache.model import CacheKeyDefinition


def create_key(prefix: str, ttl_seconds: int | None = None) -> CacheKeyDefinition:
    """Build a `CacheKeyDefinition`, validating the TTL.

    Raises:
        ValueError: when `ttl_seconds` is not a positive finite number.
    """
    if ttl_seconds is not None and (not math.isfinite(ttl_seconds) or ttl_seconds <= 0):
        raise ValueError("Cache key ttl_seconds must be a positive finite number")
    return CacheKeyDefinition(prefix=prefix, ttl_seconds=ttl_seconds)


def key(name: str, *parts: str) -> CacheKeyDefinition:
    """Convenience shorthand for `create_key` with an optional suffix chain."""
    prefix = ":".join((name, *parts)) if parts else name
    return CacheKeyDefinition(prefix=prefix)
