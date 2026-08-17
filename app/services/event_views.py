import asyncio
import logging
from collections import Counter

from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.infrastructure.database.db import DatabaseManager

logger = logging.getLogger(__name__)

EVENT_VIEW_TTL_SECONDS = 5 * 60
EVENT_VIEW_BATCH_SIZE = 10
EVENT_VIEW_FLUSH_SECONDS = 5
EVENT_VIEW_QUEUE_MAX_SIZE = 1_000


class EventViewService:
    """Дедуплицирует просмотры и передаёт уникальные события воркеру."""

    def __init__(self, redis: Redis, queue: asyncio.Queue[int | None]) -> None:
        self._redis = redis
        self._queue = queue

    async def track(self, event_id: int, ip: str) -> None:
        """Ставит уникальный просмотр в очередь, не влияя на HTTP-ответ."""
        key = f"views:{event_id}:{ip}"
        try:
            if not await self._redis.set(key, "1", nx=True, ex=EVENT_VIEW_TTL_SECONDS):
                return
        except RedisError:
            logger.exception("Не удалось дедуплицировать просмотр мероприятия")
            return

        try:
            self._queue.put_nowait(event_id)
        except asyncio.QueueFull:
            logger.exception("Очередь просмотров мероприятий заполнена")
            try:
                await self._redis.delete(key)
            except RedisError:
                logger.exception("Не удалось отменить дедупликацию просмотра мероприятия")


class EventViewWorker:
    """Агрегирует просмотры из очереди и периодически записывает их в БД."""

    def __init__(
        self,
        queue: asyncio.Queue[int | None],
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._queue = queue
        self._session_factory = session_factory

    async def run(self) -> None:
        """Обрабатывает очередь до получения сигнала остановки."""
        counts: Counter[int] = Counter()
        loop = asyncio.get_running_loop()
        flush_at: float | None = None

        while True:
            timeout = EVENT_VIEW_FLUSH_SECONDS if flush_at is None else max(0, flush_at - loop.time())
            try:
                event_id = await asyncio.wait_for(self._queue.get(), timeout)
            except TimeoutError:
                await self._flush(counts)
                flush_at = loop.time() + EVENT_VIEW_FLUSH_SECONDS if counts else None
                continue

            self._queue.task_done()
            if event_id is None:
                while not await self._flush(counts):
                    await asyncio.sleep(EVENT_VIEW_FLUSH_SECONDS)
                return

            counts[event_id] += 1
            if sum(counts.values()) >= EVENT_VIEW_BATCH_SIZE:
                await self._flush(counts)
                flush_at = loop.time() + EVENT_VIEW_FLUSH_SECONDS if counts else None
            elif flush_at is None:
                flush_at = loop.time() + EVENT_VIEW_FLUSH_SECONDS

    async def _flush(self, counts: Counter[int]) -> None:
        """Сбрасывает накопленные счётчики, сохраняя их при ошибке БД."""
        if not counts:
            return

        try:
            async with self._session_factory() as session:
                database = DatabaseManager(session, self._session_factory)
                await database.event_views.increment_many(counts)
                await database.commit()
        except Exception:
            logger.exception("Не удалось сохранить просмотры мероприятий")
            return

        counts.clear()
