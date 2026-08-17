import asyncio
from collections import Counter

import pytest
from redis.exceptions import RedisError
from sqlalchemy import select

from app.infrastructure.database.models import EventView
from app.services import event_views
from app.services.event_views import EventViewService, EventViewWorker


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.deleted: list[str] = []

    async def set(self, key: str, value: str, *, nx: bool, ex: int) -> bool | None:
        if nx and key in self.values:
            return None
        self.values[key] = value
        return True

    async def delete(self, key: str) -> None:
        self.deleted.append(key)
        self.values.pop(key, None)


async def test_track_enqueues_only_unique_event_views() -> None:
    redis = FakeRedis()
    queue: asyncio.Queue[int | None] = asyncio.Queue()
    service = EventViewService(redis, queue)

    await service.track(1, "127.0.0.1")
    await service.track(1, "127.0.0.1")
    await service.track(1, "127.0.0.2")
    await service.track(2, "127.0.0.1")

    assert [queue.get_nowait() for _ in range(queue.qsize())] == [1, 1, 2]
    assert set(redis.values) == {"views:1:127.0.0.1", "views:1:127.0.0.2", "views:2:127.0.0.1"}


async def test_track_ignores_redis_error() -> None:
    class BrokenRedis(FakeRedis):
        async def set(self, key: str, value: str, *, nx: bool, ex: int) -> bool | None:
            raise RedisError

    queue: asyncio.Queue[int | None] = asyncio.Queue()

    await EventViewService(BrokenRedis(), queue).track(1, "127.0.0.1")

    assert queue.empty()


async def test_track_cancels_deduplication_when_queue_is_full() -> None:
    redis = FakeRedis()
    queue: asyncio.Queue[int | None] = asyncio.Queue(maxsize=1)
    await queue.put(2)

    await EventViewService(redis, queue).track(1, "127.0.0.1")

    assert "views:1:127.0.0.1" not in redis.values
    assert redis.deleted == ["views:1:127.0.0.1"]


async def test_worker_flushes_after_ten_events(monkeypatch: pytest.MonkeyPatch) -> None:
    queue: asyncio.Queue[int | None] = asyncio.Queue()
    worker = EventViewWorker(queue, None)  # type: ignore[arg-type]
    flushed: list[dict[int, int]] = []

    async def flush(counts: Counter[int]) -> bool:
        if counts:
            flushed.append(dict(counts))
            counts.clear()
        return True

    monkeypatch.setattr(worker, "_flush", flush)
    for _ in range(10):
        await queue.put(1)
    await queue.put(None)

    await worker.run()

    assert flushed == [{1: 10}]


async def test_worker_flushes_after_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(event_views, "EVENT_VIEW_FLUSH_SECONDS", 0.01)
    queue: asyncio.Queue[int | None] = asyncio.Queue()
    worker = EventViewWorker(queue, None)  # type: ignore[arg-type]
    flushed = asyncio.Event()

    async def flush(counts: Counter[int]) -> bool:
        if counts:
            counts.clear()
            flushed.set()
        return True

    monkeypatch.setattr(worker, "_flush", flush)
    task = asyncio.create_task(worker.run())
    await queue.put(1)

    await asyncio.wait_for(flushed.wait(), timeout=0.1)
    await queue.put(None)
    await task


async def test_worker_flushes_remaining_events_on_stop(monkeypatch: pytest.MonkeyPatch) -> None:
    queue: asyncio.Queue[int | None] = asyncio.Queue()
    worker = EventViewWorker(queue, None)  # type: ignore[arg-type]
    flushed: list[dict[int, int]] = []

    async def flush(counts: Counter[int]) -> bool:
        if counts:
            flushed.append(dict(counts))
            counts.clear()
        return True

    monkeypatch.setattr(worker, "_flush", flush)
    await queue.put(1)
    await queue.put(2)
    await queue.put(None)

    await worker.run()

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
