import asyncio
from contextlib import suppress

from pydantic import ValidationError
from redis.asyncio import Redis
from redis.exceptions import LockNotOwnedError, RedisError

from app.domain.dto import EventReadDTO
from app.domain.exceptions import EventCacheUnavailableError, EventLoadTimeoutError, EventNotFoundError
from app.infrastructure.database.db import DatabaseManager

EVENT_CACHE_POLL_SECONDS = 0.05


class EventReadService:
    """Получает мероприятие из кэша с распределённой защитой от cache miss."""

    def __init__(
        self,
        database: DatabaseManager,
        redis: Redis,
        cache_ttl_seconds: int,
        lock_ttl_seconds: int,
        database_timeout_seconds: float,
        lock_wait_seconds: float,
    ) -> None:
        self._database = database
        self._redis = redis
        self._cache_ttl_seconds = cache_ttl_seconds
        self._lock_ttl_seconds = lock_ttl_seconds
        self._database_timeout_seconds = database_timeout_seconds
        self._lock_wait_seconds = lock_wait_seconds

    @staticmethod
    def _cache_key(event_id: int) -> str:
        return f"events:{event_id}"

    @staticmethod
    def _lock_key(event_id: int) -> str:
        return f"locks:events:{event_id}"

    async def _get_cached_event(self, event_id: int) -> EventReadDTO | None:
        cached_event = await self._redis.get(self._cache_key(event_id))
        return EventReadDTO.model_validate_json(cached_event) if cached_event else None

    async def get_event(self, event_id: int) -> EventReadDTO:
        try:
            cached_event = await self._get_cached_event(event_id)
            if cached_event:
                return cached_event

            lock = self._redis.lock(self._lock_key(event_id), timeout=self._lock_ttl_seconds)
            if await lock.acquire(blocking=False):
                try:
                    cached_event = await self._get_cached_event(event_id)
                    if cached_event:
                        return cached_event

                    try:
                        async with asyncio.timeout(self._database_timeout_seconds):
                            event = await self._database.events.get_read_by_id(event_id)
                    except TimeoutError as error:
                        raise EventLoadTimeoutError from error
                    if event is None:
                        raise EventNotFoundError
                    await self._redis.set(
                        self._cache_key(event_id),
                        event.model_dump_json(),
                        ex=self._cache_ttl_seconds,
                    )
                    return event
                finally:
                    with suppress(LockNotOwnedError, RedisError):
                        await lock.release()

            for _ in range(int(self._lock_wait_seconds / EVENT_CACHE_POLL_SECONDS)):
                await asyncio.sleep(EVENT_CACHE_POLL_SECONDS)
                cached_event = await self._get_cached_event(event_id)
                if cached_event:
                    return cached_event
            raise EventLoadTimeoutError
        except (RedisError, ValidationError) as error:
            raise EventCacheUnavailableError from error
