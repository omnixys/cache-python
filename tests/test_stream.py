"""Behavioral tests for stream consumer-group support."""

from __future__ import annotations

import fakeredis
import pytest

from cache import CacheClient, StreamMessage, ValkeyStreamService


@pytest.fixture
def client() -> CacheClient:
    return CacheClient(client=fakeredis.FakeAsyncRedis(decode_responses=True))


@pytest.fixture
def streams(client: CacheClient) -> ValkeyStreamService:
    return ValkeyStreamService(client)


async def test_ensure_group_is_idempotent(streams: ValkeyStreamService) -> None:
    await streams.ensure_group("orders", "workers")
    await streams.ensure_group("orders", "workers")


async def test_enqueue_and_consume(streams: ValkeyStreamService) -> None:
    await streams.ensure_group("orders", "workers")
    mid = await streams.enqueue("orders", {"orderId": "o1"})
    assert mid
    messages = await streams.consume("orders", "workers", "consumer-1", count=10, block_ms=100)
    assert len(messages) == 1
    message = messages[0]
    assert message.id == mid
    assert message.data == {"orderId": "o1"}


async def test_consume_returns_nothing_when_empty(streams: ValkeyStreamService) -> None:
    await streams.ensure_group("orders", "workers")
    assert await streams.consume("orders", "workers", "consumer-1", block_ms=100) == []


async def test_ack_removes_pending(streams: ValkeyStreamService) -> None:
    await streams.ensure_group("orders", "workers")
    mid = await streams.enqueue("orders", {"orderId": "o1"})
    await streams.consume("orders", "workers", "consumer-1", block_ms=100)
    pending = await streams.read_pending("orders", "workers", "consumer-1")
    assert [m.id for m in pending] == [mid]
    await streams.ack("orders", "workers", mid)
    assert await streams.read_pending("orders", "workers", "consumer-1") == []


async def test_message_types(streams: ValkeyStreamService) -> None:
    await streams.ensure_group("s", "g")
    await streams.enqueue("s", {"n": 1})
    messages = await streams.consume("s", "g", "c", block_ms=100)
    assert isinstance(messages[0], StreamMessage)
    assert isinstance(messages[0].id, str)
