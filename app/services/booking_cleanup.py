from datetime import UTC, datetime

from app.infrastructure.database.db import DatabaseManager


class BookingCleanupService:
    """Освобождает места и удаляет истёкшие pending-брони атомарно."""

    def __init__(self, database: DatabaseManager) -> None:
        self._database = database

    async def cleanup_expired(self, now: datetime | None = None) -> int:
        now = now or datetime.now(UTC).replace(tzinfo=None)
        async with self._database.transaction() as database:
            booking_ids = await database.bookings.get_expired_pending_for_update(now)
            await database.event_seats.release_reservations(booking_ids)
            return await database.bookings.delete_expired_pending(booking_ids, now)
