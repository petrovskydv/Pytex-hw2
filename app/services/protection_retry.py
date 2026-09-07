from typing import TYPE_CHECKING

from app.infrastructure.database.db import DatabaseManager

if TYPE_CHECKING:
    from app.infrastructure.api_clients.protection import ProtectionClient


class ProtectionRetryService:
    """Дорассчитывает защиту для всё ещё актуальной брони."""

    def __init__(self, database: DatabaseManager, protection_client: "ProtectionClient") -> None:
        self._database = database
        self._protection_client = protection_client

    async def calculate_for_pending_booking(self, booking_id: int) -> bool:
        async with self._database.transaction() as database:
            booking = await database.bookings.get_pending_protection(booking_id)
        if booking is None:
            return False

        protection = await self._protection_client.calculate_once(
            booking.booking_id,
            booking.amount,
            booking.event_category,
            booking.event_starts_at.isoformat(),
        )
        if not protection.available:
            return True

        async with self._database.transaction() as database:
            return await database.bookings.save_protection_if_pending(booking_id, protection.price)
