from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress

from pydantic import ValidationError
from redis.asyncio import Redis
from redis.exceptions import LockNotOwnedError, RedisError

from app.domain.dto import EventDetailsDTO
from app.domain.exceptions import EventCacheUnavailableError


class EventCache:
    """Кэш мероприятий и распределённые блокировки на Redis."""

    def __init__(self, redis: Redis, ttl_seconds: int, lock_ttl_seconds: int) -> None:
        self._redis = redis
        self._ttl_seconds = ttl_seconds
        self._lock_ttl_seconds = lock_ttl_seconds

    async def get(self, event_id: int) -> EventDetailsDTO | None:
        try:
            cached_event = await self._redis.get(self._cache_key(event_id))
            return EventDetailsDTO.model_validate_json(cached_event) if cached_event else None
        except (RedisError, ValidationError) as error:
            raise EventCacheUnavailableError from error

    async def set(self, event: EventDetailsDTO) -> None:
        try:
            await self._redis.set(
                self._cache_key(event.id),
                event.model_dump_json(),
                ex=self._ttl_seconds,
            )
        except RedisError as error:
            raise EventCacheUnavailableError from error

    @asynccontextmanager
    async def acquire_lock(self, event_id: int) -> AsyncIterator[bool]:
        try:
            lock = self._redis.lock(self._lock_key(event_id), timeout=self._lock_ttl_seconds)
            acquired = await lock.acquire(blocking=False)
            try:
                yield acquired
            finally:
                if acquired:
                    with suppress(LockNotOwnedError):
                        await lock.release()
        except RedisError as error:
            raise EventCacheUnavailableError from error

    @staticmethod
    def _cache_key(event_id: int) -> str:
        return f"events:{event_id}"

    @staticmethod
    def _lock_key(event_id: int) -> str:
        return f"locks:events:{event_id}"
