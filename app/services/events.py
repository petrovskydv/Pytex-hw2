from datetime import UTC, datetime, timedelta

from app.domain.dto import BookingDTO, EventSeatDTO
from app.domain.exceptions import EventNotFoundError, SeatsNotFoundError, SeatsUnavailableError
from app.infrastructure.database.db import DatabaseManager


class EventService:
    def __init__(
        self,
        database: DatabaseManager,
        booking_ttl_minutes: int,
    ) -> None:
        self._database = database
        self._booking_ttl_minutes = booking_ttl_minutes

    async def create_checkout_booking(self, event_id: int, user_id: int, seat_ids: list[int]) -> BookingDTO:
        async with self._database.transaction() as database:
            if not await database.events.exists(event_id):
                raise EventNotFoundError
            if await database.event_seats.get_existing_seat_ids(event_id, seat_ids) != set(seat_ids):
                raise SeatsNotFoundError

            event_seats: list[EventSeatDTO] = await database.event_seats.get_available_for_update(event_id, seat_ids)
            if len(event_seats) != len(seat_ids) or {seat.seat_id for seat in event_seats} != set(seat_ids):
                raise SeatsUnavailableError

            reserved_until = datetime.now(UTC).replace(tzinfo=None) + timedelta(minutes=self._booking_ttl_minutes)
            booking = await database.bookings.create(
                event_id=event_id,
                user_id=user_id,
                amount=sum(event_seat.price for event_seat in event_seats),
                reserved_until=reserved_until,
            )
            await database.event_seats.reserve(
                event_seat_ids=[event_seat.id for event_seat in event_seats],
                booking_id=booking.id,
                reserved_until=reserved_until,
            )
            return booking
