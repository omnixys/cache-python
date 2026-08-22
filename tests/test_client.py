"""Behavioral tests for the cache client and serializers."""

from __future__ import annotations

import fakeredis
import pytest

from cache import CacheClient, CacheKeyDefinition, JsonCacheSerializer, StrictJsonCacheSerializer


@pytest.fixture
def client() -> CacheClient:
    return CacheClient(client=fakeredis.FakeAsyncRedis(decode_responses=True))


async def test_set_get_delete(client: CacheClient) -> None:
    await client.set("name", "ada", ttl_seconds=60)
    assert await client.get("name") == "ada"
    assert await client.exists("name") is True
    assert await client.ttl("name") > 0
    assert await client.delete("name") == 1
    assert await client.get("name") is None


async def test_key_prefixing(client: CacheClient) -> None:
    prefixed = CacheClient(client=client.raw, key_prefix="svc")
    await prefixed.set("user:1", "x")
    assert await client.get("user:1") is None
    assert await client.raw.get("svc:user:1") == "x"


async def test_key_definition(client: CacheClient) -> None:
    definition = CacheKeyDefinition(prefix="token", ttl_seconds=30)
    assert definition.key("user", "42") == "token:user:42"
    await client.set(definition, "abc", None, "user", "42")
    assert await client.raw.get("token:user:42") == "abc"
    await client.expire(definition, 5, "user", "42")
    assert await client.ttl(definition, "user", "42") > 0


async def test_key_definition_default_ttl(client: CacheClient) -> None:
    definition = CacheKeyDefinition(prefix="pin", ttl_seconds=10)
    await client.set(definition, "1234")
    assert await client.ttl(definition) > 0


async def test_json_roundtrip(client: CacheClient) -> None:
    payload = {"user": {"id": 1}, "tags": ["a", "b"]}
    await client.set_json("user:1", payload, ttl_seconds=60)
    assert await client.get_json("user:1") == payload


async def test_json_deserialize_invalid_returns_none(client: CacheClient) -> None:
    await client.raw.set("bad", "not json{")
    assert await client.get_json("bad") is None


async def test_increment_with_ttl(client: CacheClient) -> None:
    assert await client.increment("counter", ttl_seconds=10) == 1
    assert await client.increment("counter", ttl_seconds=10) == 2
    assert await client.ttl("counter") > 0


async def test_set_if_not_exists(client: CacheClient) -> None:
    assert await client.set_if_not_exists("once", "v") is True
    assert await client.set_if_not_exists("once", "v2") is False
    assert await client.get("once") == "v"


async def test_set_nx_px(client: CacheClient) -> None:
    assert await client.set_nx_px("lock:key", "token", 3000) is True
    assert await client.set_nx_px("lock:key", "other", 3000) is False


def test_serializers() -> None:
    serializer = JsonCacheSerializer()
    assert serializer.serialize({"a": 1}) == '{"a": 1}'
    assert serializer.deserialize('{"a": 1}') == {"a": 1}
    assert serializer.deserialize("garbage") is None
    strict = StrictJsonCacheSerializer()
    assert strict.deserialize('{"a": 1}') == {"a": 1}
    with pytest.raises(ValueError):
        strict.deserialize("garbage")


async def test_ping(client: CacheClient) -> None:
    assert await client.ping() is True
