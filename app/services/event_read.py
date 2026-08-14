import asyncio

from app.domain.dto import EventDetailsDTO
from app.domain.exceptions import EventLoadTimeoutError, EventNotFoundError
from app.infrastructure.cache.event_cache import EventCache
from app.infrastructure.database.db import DatabaseManager

EVENT_CACHE_POLL_SECONDS = 0.05


class EventReadService:
    """Сервис чтения информации о мероприятиях."""

    def __init__(
        self,
        database: DatabaseManager,
        event_cache: EventCache,
        database_timeout_seconds: float,
        lock_wait_seconds: float,
    ) -> None:
        self._database = database
        self._event_cache = event_cache
        self._database_timeout_seconds = database_timeout_seconds
        self._lock_wait_seconds = lock_wait_seconds

    async def _get_cached_event(self, event_id: int) -> EventDetailsDTO | None:
        """Возвращает мероприятие из кэша."""
        return await self._event_cache.get(event_id)

    async def _load_and_cache_event(self, event_id: int) -> EventDetailsDTO:
        """Загружает мероприятие из БД и сохраняет его в кэше."""
        if cached_event := await self._get_cached_event(event_id):
            return cached_event

        try:
            async with asyncio.timeout(self._database_timeout_seconds):
                event = await self._database.events.get_by_id(event_id)
        except TimeoutError as error:
            raise EventLoadTimeoutError from error
        if event is None:
            raise EventNotFoundError
        await self._event_cache.set(event)
        return event

    async def _wait_for_cached_event(self, event_id: int) -> EventDetailsDTO:
        """Ожидает заполнения кэша лидером загрузки."""
        for _ in range(int(self._lock_wait_seconds / EVENT_CACHE_POLL_SECONDS)):
            await asyncio.sleep(EVENT_CACHE_POLL_SECONDS)
            cached_event = await self._get_cached_event(event_id)
            if cached_event:
                return cached_event
        raise EventLoadTimeoutError

    async def _try_load_event_with_lock(self, event_id: int) -> EventDetailsDTO | None:
        """Загружает мероприятие, если удалось стать лидером заполнения кэша."""
        async with self._event_cache.acquire_lock(event_id) as is_leader:
            if is_leader:
                return await self._load_and_cache_event(event_id)
        return None

    async def get_event(self, event_id: int) -> EventDetailsDTO:
        """Возвращает мероприятие, предотвращая одновременную загрузку из БД."""
        if cached_event := await self._get_cached_event(event_id):
            return cached_event

        if event := await self._try_load_event_with_lock(event_id):
            return event

        try:
            return await self._wait_for_cached_event(event_id)
        except EventLoadTimeoutError:
            # Лидер мог завершиться с ошибкой и освободить блокировку без записи в кэш.
            if event := await self._try_load_event_with_lock(event_id):
                return event
            raise
