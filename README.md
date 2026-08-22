# omnixys-cache

Omnixys shared cache package — a typed Redis/Valkey facade with JSON serialization, invalidation, distributed locks, rate limiting, streams with consumer groups, and delayed jobs, all on one client.

## Installation

```bash
pip install omnixys-cache
```

## Features

- **CacheClient**: async Redis/Valkey facade with typed keys, JSON serialization, TTL handling, atomic `NX`/`PX` ops and low-level `raw` access
- **Key definitions**: `create_key(prefix, ttl_seconds)` and `key(...)` builders validated at construction time
- **CacheLock**: distributed mutual-exclusion lock with token-based (WATCH/MULTI) release
- **RateLimiter**: fixed-window rate limiting with `hit`/`check` and TTL window reset
- **ValkeyStreamService**: stream + consumer-group queues (`ensureGroup`, `enqueue`, `consume`, `read_pending`, `ack`)
- **Delayed jobs**: schedule/claim/retry/cancel delayed jobs with a worker loop and decorator-driven registry
- **Invalidation**: `CacheInvalidationService` with pluggable strategies
- **Serialization**: JSON serializers (`JsonCacheSerializer`, `StrictJsonCacheSerializer`)
- **DI**: `CacheProvider` container integration (dishka)

## Usage

### Client and keys

```python
import asyncio
from cache import CacheClient, create_key

session = create_key("session", ttl_seconds=300)
cache = CacheClient(url="redis://localhost:6379", key_prefix="myapp")

async def main() -> None:
    await cache.set_json(session, {"userId": 42}, parts=("u1",))  # myapp:session:u1
    assert await cache.get_json(session, "u1") == {"userId": 42}
    await cache.expire(session, 60, "u1")
    await cache.close()

asyncio.run(main())
```

Raw keys work too — pass a plain `str` and optional parts are joined with `:`.

### Distributed lock

```python
from cache import CacheClient, CacheLock

async with CacheLock(cache, "deploy:eu1", ttl_ms=30_000, wait_ms=10_000) as lock:
    ...
# or manually:
lock = CacheLock(cache, "deploy:eu1", ttl_ms=30_000)
if await lock.acquire():
    try:
        ...
    finally:
        await lock.release()
```

### Rate limiting

```python
from cache import CacheClient, RateLimiter

limiter = RateLimiter(cache)
if not await limiter.check("sms:user:42", limit=5, ttl_seconds=60).allowed:
    raise RuntimeError("rate limit exceeded")
```

### Streams with consumer groups

```python
from cache import CacheClient, ValkeyStreamService

streams = ValkeyStreamService(cache)
await streams.ensure_group("jobs", "workers")

await streams.enqueue("jobs", {"type": "resize", "imageId": "abc"})

for message in await streams.consume("jobs", "workers", consumer="w1", block_ms=1_000):
    payload = message.data          # {"type": "resize", "imageId": "abc"}
    await streams.ack("jobs", "workers", message.id)
```

### Delayed jobs

```python
from cache import DelayedJobService, DelayedJobSchedule, DelayedJobWorker, delayed_job

@delayed_job("image.resize")
async def resize(payload: dict) -> None:
    print(payload["imageId"])

service = DelayedJobService(cache)
job_id = await service.schedule(
    DelayedJobSchedule(type="image.resize", payload={"imageId": "abc"}, delay_ms=5_000)
)

worker = DelayedJobWorker(service)          # uses the default (decorated) registry
await worker.start()
```

Jobs are persisted as records (7-day TTL), scheduled on a sorted set (`delayed:jobs`), claimed atomically via WATCH/MULTI, retried on failure up to `max_retries`, and surfaced through `status()`, `cancel()`, `retry()`, and `claim_due()`.

## Testing

The package ships behavior-level tests (pytest + fakeredis, no server needed):

```bash
uv run pytest -q          # 47 tests
uv run ruff check .       # lint
uv run mypy src/          # strict typing
```

## License

GPL-3.0-or-later
