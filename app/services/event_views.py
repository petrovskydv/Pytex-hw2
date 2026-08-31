import asyncio
import logging

from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.domain.exceptions import EventViewSaveError
from app.infrastructure.database.db import DatabaseManager

logger = logging.getLogger(__name__)

EVENT_VIEW_TTL_SECONDS = 5 * 60


class EventViewQueue:
    """Управляет очередью просмотров событий с lifecycle методами."""

    def __init__(
        self,
        redis: Redis,
        session_factory: async_sessionmaker[AsyncSession],
        batch_size: int = 10,
        flush_seconds: float = 5,
    ) -> None:
        self._redis = redis
        self._session_factory = session_factory
        self._batch_size = batch_size
        self._flush_seconds = flush_seconds
        self._queue: asyncio.Queue[int | None] = asyncio.Queue()
        self._worker_task: asyncio.Task[None] | None = None

    def start(self) -> None:
        """Запускает фонового воркера."""
        if self._worker_task is None:
            self._worker_task = asyncio.create_task(self._run_worker())

    async def stop(self) -> None:
        """Останавливает воркера, флушит оставшиеся события."""
        if self._worker_task:
            await self._queue.put(None)
            await self._worker_task
            self._worker_task = None

    async def add_event_view(self, event_id: int, ip: str) -> None:
        """Добавляет уникальный просмотр в очередь."""
        try:
            key = f"views:{event_id}:{ip}"
            if await self._redis.set(key, "1", nx=True, ex=EVENT_VIEW_TTL_SECONDS):
                self._queue.put_nowait(event_id)
        except RedisError:
            logger.exception("Redis error in add_event_view")

    async def _run_worker(self) -> None:
        """Фоновый воркер для батчинга событий."""
        events: list[int] = []
        flush_at = 0.0
        while True:
            try:
                timeout = None
                if events:
                    timeout = max(0, flush_at - asyncio.get_running_loop().time())

                event_id = await asyncio.wait_for(self._queue.get(), timeout=timeout)

                if event_id is None:
                    # Graceful shutdown
                    if events:
                        await self._flush_events(events)
                    return

                events.append(event_id)
                if len(events) == 1:
                    flush_at = asyncio.get_running_loop().time() + self._flush_seconds
                if len(events) >= self._batch_size:
                    await self._flush_events(events)
                    events = []

            except TimeoutError:
                if events:
                    await self._flush_events(events)
                    events = []

    async def _flush_events(self, events: list[int]) -> None:
        """Агрегирует и сохраняет события в БД."""
        if not events:
            return

        # Агрегация событий
        counts: dict[int, int] = {}
        for event_id in events:
            counts[event_id] = counts.get(event_id, 0) + 1

        # Сохранение в БД
        try:
            async with self._session_factory() as session:
                database = DatabaseManager(session, self._session_factory)
                await database.event_views.increment_many(counts)
                await database.commit()
        except EventViewSaveError:
            logger.exception("Failed to save event views")
