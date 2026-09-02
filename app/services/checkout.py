import asyncio
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from app.domain.dto import CheckoutDTO, EventCheckoutDTO, EventSeatDTO, PaymentCalculationDTO, ProtectionCalculationDTO
from app.domain.exceptions import EventNotFoundError, PaymentCalculationError, SeatsNotFoundError, SeatsUnavailableError
from app.infrastructure.database.db import DatabaseManager

if TYPE_CHECKING:
    from app.infrastructure.api_clients.payment import PaymentClient
    from app.infrastructure.api_clients.protection import ProtectionClient
    from app.services.task_dispatchers import ProtectionRetryDispatcher


class CheckoutService:
    """Создаёт бронирования и рассчитывает условия их оформления."""

    def __init__(
        self,
        database: DatabaseManager,
        booking_ttl_minutes: int,
        payment_client: "PaymentClient",
        protection_client: "ProtectionClient",
        protection_retry_dispatcher: "ProtectionRetryDispatcher",
    ) -> None:
        self._database = database
        self._booking_ttl_minutes = booking_ttl_minutes
        self._payment_client = payment_client
        self._protection_client = protection_client
        self._protection_retry_dispatcher = protection_retry_dispatcher

    async def _get_event(
        self,
        database: DatabaseManager,
        event_id: int,
    ) -> EventCheckoutDTO:
        event = await database.events.get_by_id(event_id)
        if event is None:
            raise EventNotFoundError
        return EventCheckoutDTO.model_validate(event)

    async def _ensure_checkout_seats_exist(
        self,
        database: DatabaseManager,
        event_id: int,
        seat_ids: list[int],
    ) -> None:
        """Убедиться, что все указанные места существуют у события."""
        if await database.event_seats.get_existing_seat_ids(event_id, seat_ids) != set(seat_ids):
            raise SeatsNotFoundError

    async def _calculate_checkout(
        self,
        booking_id: int,
        amount: int,
        event: EventCheckoutDTO,
    ) -> tuple[PaymentCalculationDTO, ProtectionCalculationDTO | None]:
        """Рассчитать платёж и страховку параллельно; отменить расчет страховки при ошибке расчета платежа."""
        payment_task = asyncio.create_task(self._payment_client.calculate(booking_id, amount))
        protection_task = asyncio.create_task(
            self._protection_client.calculate(
                booking_id,
                amount,
                event.category,
                event.starts_at.isoformat(),
            )
        )
        try:
            payment = await payment_task
        except BaseException:
            protection_task.cancel()
            await asyncio.gather(protection_task, return_exceptions=True)
            raise
        return payment, await protection_task

    async def create_checkout_booking(self, event_id: int, user_id: int, seat_ids: list[int]) -> CheckoutDTO:
        """Зарезервировать места и сохранить расчёт условий оформления."""
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

        try:
            payment, protection = await self._calculate_checkout(booking.id, booking.amount, event)
        except PaymentCalculationError:
            # Резерв уже сохранён, поэтому при ошибке расчёта освобождаем места отдельной транзакцией.
            async with self._database.transaction() as database:
                await database.bookings.cancel(booking.id)
                await database.event_seats.release_reservation(booking.id)
            raise
        protection_price = protection.price if protection and protection.available else None

        await self._database.bookings.save_checkout_calculation(booking.id, payment.commission, protection_price)
        await self._database.commit()
        if protection is None:
            await self._protection_retry_dispatcher.enqueue(booking.id)

        return CheckoutDTO(
            booking=booking,
            event=event,
            seats=event_seats,
            payment=payment,
            protection=protection,
        )
