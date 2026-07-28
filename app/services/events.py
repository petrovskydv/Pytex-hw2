from datetime import UTC, datetime, timedelta

from app.domain.dto import BookingDTO, EventSeatDTO
from app.domain.exceptions import EventNotFoundError, SeatsNotFoundError, SeatsUnavailableError
from app.repositories.booking import BookingRepository
from app.repositories.event import EventRepository
from app.repositories.event_seat import EventSeatRepository


class EventService:
    def __init__(
        self,
        booking_repository: BookingRepository,
        event_repository: EventRepository,
        event_seat_repository: EventSeatRepository,
        booking_ttl_minutes: int,
    ) -> None:
        self._booking_repository = booking_repository
        self._event_repository = event_repository
        self._event_seat_repository = event_seat_repository
        self._booking_ttl_minutes = booking_ttl_minutes

    async def get_available_seats_for_checkout(self, event_id: int, seat_ids: list[int]) -> list[EventSeatDTO]:
        event_seats = await self._event_seat_repository.get_available_for_update(event_id, seat_ids)
        if len(event_seats) != len(seat_ids) or {seat.seat_id for seat in event_seats} != set(seat_ids):
            raise SeatsUnavailableError
        return event_seats

    async def ensure_checkout_targets_exist(self, event_id: int, seat_ids: list[int]) -> None:
        if not await self._event_repository.exists(event_id):
            raise EventNotFoundError
        if await self._event_seat_repository.get_existing_seat_ids(event_id, seat_ids) != set(seat_ids):
            raise SeatsNotFoundError

    async def create_checkout_booking(self, event_id: int, user_id: int, seat_ids: list[int]) -> BookingDTO:
        await self.ensure_checkout_targets_exist(event_id, seat_ids)
        event_seats = await self.get_available_seats_for_checkout(event_id, seat_ids)
        reserved_until = datetime.now(UTC).replace(tzinfo=None) + timedelta(minutes=self._booking_ttl_minutes)
        booking = await self._booking_repository.create(
            event_id=event_id,
            user_id=user_id,
            amount=sum(event_seat.price for event_seat in event_seats),
            reserved_until=reserved_until,
        )
        await self._event_seat_repository.reserve(
            event_seat_ids=[event_seat.id for event_seat in event_seats],
            booking_id=booking.id,
            reserved_until=reserved_until,
        )
        return booking
