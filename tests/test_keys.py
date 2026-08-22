"""Behavioral tests for cache key definitions."""

from __future__ import annotations

import pytest

from cache import CacheKeyDefinition, create_key, key


def test_create_key_builds_definition() -> None:
    definition = create_key("session", ttl_seconds=60)
    assert isinstance(definition, CacheKeyDefinition)
    assert definition.prefix == "session"
    assert definition.ttl_seconds == 60
    assert definition.key("user", "42") == "session:user:42"


def test_create_key_defaults_no_ttl() -> None:
    definition = create_key("plain")
    assert definition.ttl_seconds is None


def test_create_key_validates_ttl() -> None:
    with pytest.raises(ValueError):
        create_key("bad", ttl_seconds=0)
    with pytest.raises(ValueError):
        create_key("bad", ttl_seconds=-5)
    with pytest.raises(ValueError):
        create_key("bad", ttl_seconds=float("nan"))


def test_key_shorthand() -> None:
    definition = key("tenant", "t1")
    assert definition.prefix == "tenant:t1"
    assert definition.key("k") == "tenant:t1:k"
