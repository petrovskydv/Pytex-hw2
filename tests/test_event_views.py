import asyncio

import pytest
from redis.exceptions import RedisError
from sqlalchemy import select

from app.infrastructure.database.models import EventView
from app.services.event_views import EventViewQueue


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    async def set(self, key: str, value: str, *, nx: bool, ex: int) -> bool | None:
        if nx and key in self.values:
            return None
        self.values[key] = value
        return True


async def test_add_event_view_enqueues_only_unique() -> None:
    redis = FakeRedis()
    queue = EventViewQueue(redis, None)  # type: ignore[arg-type]

    await queue.add_event_view(1, "127.0.0.1")
    await queue.add_event_view(1, "127.0.0.1")
    await queue.add_event_view(1, "127.0.0.2")
    await queue.add_event_view(2, "127.0.0.1")

    items = [queue._queue.get_nowait() for _ in range(queue._queue.qsize())]
    assert items == [1, 1, 2]
    assert set(redis.values) == {"views:1:127.0.0.1", "views:1:127.0.0.2", "views:2:127.0.0.1"}


async def test_add_event_view_ignores_redis_error() -> None:
    class BrokenRedis(FakeRedis):
        async def set(self, key: str, value: str, *, nx: bool, ex: int) -> bool | None:
            raise RedisError

    queue = EventViewQueue(BrokenRedis(), None)  # type: ignore[arg-type]

    await queue.add_event_view(1, "127.0.0.1")

    assert queue._queue.empty()


async def test_flushes_after_batch_size(monkeypatch: pytest.MonkeyPatch) -> None:
    flushed: list[dict[int, int]] = []

    class DummyQueue(EventViewQueue):
        async def _flush_events(self, events: list[int]) -> None:
            counts: dict[int, int] = {}
            for event_id in events:
                counts[event_id] = counts.get(event_id, 0) + 1
            flushed.append(counts)

    redis = FakeRedis()
    queue = DummyQueue(redis, None)  # type: ignore[arg-type]
    queue.start()

    for _ in range(10):
        await queue._queue.put(1)

    await asyncio.sleep(0.05)
    await queue.stop()

    assert flushed == [{1: 10}]


async def test_flushes_after_timeout() -> None:
    flushed = asyncio.Event()

    class DummyQueue(EventViewQueue):
        async def _flush_events(self, events: list[int]) -> None:
            flushed.set()

    redis = FakeRedis()
    queue = DummyQueue(redis, None, flush_seconds=0.01)  # type: ignore[arg-type]
    queue.start()

    await queue._queue.put(1)
    await asyncio.wait_for(flushed.wait(), timeout=0.1)
    await queue.stop()


async def test_flushes_remaining_events_on_stop() -> None:
    flushed: list[dict[int, int]] = []

    class DummyQueue(EventViewQueue):
        async def _flush_events(self, events: list[int]) -> None:
            counts: dict[int, int] = {}
            for event_id in events:
                counts[event_id] = counts.get(event_id, 0) + 1
            flushed.append(counts)

    redis = FakeRedis()
    queue = DummyQueue(redis, None)  # type: ignore[arg-type]
    queue.start()

    await queue._queue.put(1)
    await queue._queue.put(2)
    await asyncio.sleep(0.02)
    await queue.stop()

    assert flushed == [{1: 1, 2: 1}]


async def test_event_view_repository_accumulates_counts(database_manager, event_with_seat) -> None:
    event, _, _ = event_with_seat

    await database_manager.event_views.increment_many({event.id: 3})
    await database_manager.commit()
    await database_manager.event_views.increment_many({event.id: 2})
    await database_manager.commit()

    views_count = await database_manager._session.scalar(
        select(EventView.views_count).where(EventView.event_id == event.id)
    )

    assert views_count == 5
