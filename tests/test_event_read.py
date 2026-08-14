import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from redis.exceptions import RedisError

from app.domain.dto import EventDetailsDTO
from app.domain.exceptions import EventCacheUnavailableError, EventLoadTimeoutError, EventNotFoundError
from app.infrastructure.cache.event_cache import EventCache
from app.services.event_read import EventReadService


class FakeLock:
    def __init__(self, redis: "FakeRedis", name: str) -> None:
        self._redis = redis
        self._name = name

    async def acquire(self, *, blocking: bool) -> bool:
        if self._name in self._redis.locks:
            self._redis.failed_lock_acquires += 1
            return False
        self._redis.locks.add(self._name)
        return True

    async def release(self) -> None:
        self._redis.locks.remove(self._name)


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.locks: set[str] = set()
        self.failed_lock_acquires = 0

    async def get(self, key: str) -> str | None:
        return self.values.get(key)

    async def set(self, key: str, value: str, *, ex: int) -> None:
        self.values[key] = value

    def lock(self, name: str, *, timeout: int) -> FakeLock:
        return FakeLock(self, name)


class RedisErrorFakeRedis(FakeRedis):
    async def get(self, key: str) -> str | None:
        raise RedisError


class ReleaseErrorFakeLock(FakeLock):
    async def release(self) -> None:
        raise RedisError


class ReleaseErrorFakeRedis(FakeRedis):
    def lock(self, name: str, *, timeout: int) -> ReleaseErrorFakeLock:
        return ReleaseErrorFakeLock(self, name)


@pytest.fixture
def event() -> EventDetailsDTO:
    return EventDetailsDTO(
        id=1,
        organizer_id=2,
        location_id=3,
        title="Test event",
        description="Description",
        category="conference",
        starts_at="2030-01-01T00:00:00",
        base_price=5000,
    )


def make_service(
    redis: FakeRedis,
    get_by_id: AsyncMock,
    *,
    lock_wait_seconds: float = 1,
) -> EventReadService:
    database = SimpleNamespace(events=SimpleNamespace(get_by_id=get_by_id))
    return EventReadService(database, EventCache(redis, 300, 15), 5, lock_wait_seconds)


async def test_get_event_returns_cached_value_without_database_request(event: EventDetailsDTO) -> None:
    redis = FakeRedis()
    redis.values["events:1"] = event.model_dump_json()
    get_by_id = AsyncMock()

    result = await make_service(redis, get_by_id).get_event(1)

    assert result == event
    get_by_id.assert_not_awaited()


async def test_get_event_caches_database_value(event: EventDetailsDTO) -> None:
    redis = FakeRedis()
    get_by_id = AsyncMock(return_value=event)

    result = await make_service(redis, get_by_id).get_event(1)

    assert result == event
    assert EventDetailsDTO.model_validate_json(redis.values["events:1"]) == event
    get_by_id.assert_awaited_once_with(1)


async def test_concurrent_cache_miss_loads_event_once(event: EventDetailsDTO) -> None:
    redis = FakeRedis()
    database_started = asyncio.Event()
    release_database = asyncio.Event()

    async def get_by_id(event_id: int) -> EventDetailsDTO:
        database_started.set()
        await release_database.wait()
        return event

    database_method = AsyncMock(side_effect=get_by_id)
    service = make_service(redis, database_method)
    leader = asyncio.create_task(service.get_event(1))
    await database_started.wait()
    followers = [asyncio.create_task(service.get_event(1)) for _ in range(3)]
    await asyncio.sleep(0)
    release_database.set()

    assert await asyncio.gather(leader, *followers) == [event] * 4
    database_method.assert_awaited_once_with(1)
    assert redis.failed_lock_acquires == 3


async def test_event_not_found_releases_lock() -> None:
    redis = FakeRedis()
    service = make_service(redis, AsyncMock(return_value=None))

    with pytest.raises(EventNotFoundError):
        await service.get_event(1)

    assert redis.locks == set()


async def test_follower_times_out_when_leader_does_not_fill_cache() -> None:
    redis = FakeRedis()
    redis.locks.add("locks:events:1")
    service = make_service(redis, AsyncMock(), lock_wait_seconds=0.05)

    with pytest.raises(EventLoadTimeoutError):
        await service.get_event(1)


async def test_follower_loads_event_after_leader_error(event: EventDetailsDTO) -> None:
    redis = FakeRedis()
    database_started = asyncio.Event()
    release_database = asyncio.Event()
    attempts = 0

    async def get_by_id(event_id: int) -> EventDetailsDTO:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            database_started.set()
            await release_database.wait()
            raise RuntimeError
        return event

    database_method = AsyncMock(side_effect=get_by_id)
    service = make_service(redis, database_method, lock_wait_seconds=0.05)
    leader = asyncio.create_task(service.get_event(1))
    await database_started.wait()
    follower = asyncio.create_task(service.get_event(1))
    await asyncio.sleep(0)
    release_database.set()

    with pytest.raises(RuntimeError):
        await leader
    assert await follower == event
    assert database_method.await_count == 2


async def test_different_events_use_independent_locks(event: EventDetailsDTO) -> None:
    redis = FakeRedis()
    database_started = asyncio.Event()
    release_database = asyncio.Event()

    async def get_by_id(event_id: int) -> EventDetailsDTO:
        database_started.set()
        await release_database.wait()
        return event.model_copy(update={"id": event_id})

    database_method = AsyncMock(side_effect=get_by_id)
    service = make_service(redis, database_method)
    first = asyncio.create_task(service.get_event(1))
    await database_started.wait()
    second = asyncio.create_task(service.get_event(2))
    await asyncio.sleep(0)

    assert database_method.await_count == 2
    release_database.set()
    assert [result.id for result in await asyncio.gather(first, second)] == [1, 2]


async def test_database_error_releases_lock() -> None:
    redis = FakeRedis()
    service = make_service(redis, AsyncMock(side_effect=RuntimeError))

    with pytest.raises(RuntimeError):
        await service.get_event(1)

    assert redis.locks == set()


async def test_cancelled_database_request_releases_lock() -> None:
    redis = FakeRedis()
    database_started = asyncio.Event()

    async def get_by_id(event_id: int) -> EventDetailsDTO:
        database_started.set()
        await asyncio.Event().wait()
        return EventDetailsDTO.model_construct()

    task = asyncio.create_task(make_service(redis, AsyncMock(side_effect=get_by_id)).get_event(1))
    await database_started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert redis.locks == set()


async def test_invalid_cached_event_raises_cache_unavailable() -> None:
    redis = FakeRedis()
    redis.values["events:1"] = "invalid"

    with pytest.raises(EventCacheUnavailableError):
        await make_service(redis, AsyncMock()).get_event(1)


async def test_redis_error_raises_cache_unavailable() -> None:
    with pytest.raises(EventCacheUnavailableError):
        await make_service(RedisErrorFakeRedis(), AsyncMock()).get_event(1)


async def test_lock_release_error_raises_cache_unavailable(event: EventDetailsDTO) -> None:
    with pytest.raises(EventCacheUnavailableError):
        await make_service(ReleaseErrorFakeRedis(), AsyncMock(return_value=event)).get_event(1)
