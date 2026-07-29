import asyncio
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from app.domain.dto import CheckoutDTO, CheckoutEventDTO, EventSeatDTO
from app.domain.exceptions import EventNotFoundError, SeatsNotFoundError, SeatsUnavailableError
from app.infrastructure.database.db import DatabaseManager

if TYPE_CHECKING:
    from app.infrastructure.payment import PaymentClient
    from app.infrastructure.protection import ProtectionClient


class EventService:
    def __init__(
        self,
        database: DatabaseManager,
        booking_ttl_minutes: int,
        payment_client: "PaymentClient",
        protection_client: "ProtectionClient",
    ) -> None:
        self._database = database
        self._booking_ttl_minutes = booking_ttl_minutes
        self._payment_client = payment_client
        self._protection_client = protection_client

    async def _get_event(
        self,
        database: DatabaseManager,
        event_id: int,
    ) -> CheckoutEventDTO:
        event = await database.events.get_by_id(event_id)
        if event is None:
            raise EventNotFoundError
        return event

    async def _ensure_checkout_seats_exist(
        self,
        database: DatabaseManager,
        event_id: int,
        seat_ids: list[int],
    ) -> None:
        if await database.event_seats.get_existing_seat_ids(event_id, seat_ids) != set(seat_ids):
            raise SeatsNotFoundError

    async def create_checkout_booking(self, event_id: int, user_id: int, seat_ids: list[int]) -> CheckoutDTO:
        async with self._database.transaction() as database:
            event = await self._get_event(database, event_id)
            await self._ensure_checkout_seats_exist(database, event_id, seat_ids)

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

        payment, protection = await asyncio.gather(
            self._payment_client.calculate(booking.id, booking.amount),
            self._protection_client.calculate(
                booking.id,
                booking.amount,
                event.category,
                event.starts_at.isoformat(),
            ),
        )
        protection_price = protection.price if protection and protection.available else None

        await self._database.bookings.save_checkout_quote(booking.id, payment.commission, protection_price)
        await self._database.commit()

        return CheckoutDTO(
            booking=booking,
            event=event,
            seats=event_seats,
            payment=payment,
            protection=protection,
        )
