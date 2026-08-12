import asyncio
from datetime import UTC, datetime

import pytest
from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.routes.events import prepare_checkout
from app.api.schemas import BookingCreate, CheckoutResponse
from app.domain.dto import PaymentCalculationDTO, ProtectionCalculationDTO
from app.domain.exceptions import EventNotFoundError, PaymentCalculationError, SeatsNotFoundError
from app.domain.statuses import BookingStatus, SeatStatus
from app.infrastructure.database.models import Booking, EventSeat
from app.services.events import EventService


async def test_rejects_missing_event(database_manager, payment_client, protection_client) -> None:
    """Checkout отклоняется, если мероприятия нет в пустой базе."""
    service = EventService(database_manager, 15, payment_client, protection_client)

    with pytest.raises(EventNotFoundError):
        await service.create_checkout_booking(1, 1, [1])


async def test_rejects_missing_event_seat(
    database_manager,
    event_with_seat,
    payment_client,
    protection_client,
) -> None:
    """Checkout отклоняется, если указанное место не принадлежит мероприятию."""
    event, _, event_seat = event_with_seat
    service = EventService(database_manager, 15, payment_client, protection_client)

    with pytest.raises(SeatsNotFoundError):
        await service.create_checkout_booking(event.id, 1, [event_seat.seat_id, event_seat.seat_id + 1])


async def test_transaction_commits_on_success(database_manager, event_with_seat, session_factory) -> None:
    """Успешная транзакция сохраняет созданную бронь."""
    event, _, _ = event_with_seat
    reserved_until = datetime.now(UTC).replace(tzinfo=None)

    async with database_manager.transaction() as database:
        booking = await database.bookings.create(event.id, 1, 5000, reserved_until)

    async with session_factory() as session:
        saved_booking = await session.get(Booking, booking.id)

    assert saved_booking is not None


async def test_transaction_rolls_back_on_error(database_manager, event_with_seat, session_factory) -> None:
    """Транзакция откатывает созданную бронь при исключении."""
    event, _, _ = event_with_seat

    with pytest.raises(RuntimeError):
        async with database_manager.transaction() as database:
            await database.bookings.create(event.id, 1, 5000, datetime.now(UTC).replace(tzinfo=None))
            raise RuntimeError

    async with session_factory() as session:
        bookings = (await session.scalars(select(Booking))).all()

    assert bookings == []


async def test_checkout_saves_payment_and_protection_calculation(
    database_manager,
    event_with_seat,
    payment_client,
    protection_client,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Checkout сохраняет бронь, резерв места и результаты внешних расчетов."""
    event, _, event_seat = event_with_seat
    service = EventService(database_manager, 15, payment_client, protection_client)

    checkout = await service.create_checkout_booking(event.id, 1, [event_seat.seat_id])

    async with session_factory() as session:
        booking = await session.get(Booking, checkout.booking.id)
        saved_event_seat = await session.get(EventSeat, event_seat.id)

    assert booking is not None
    assert booking.payment_commission == 150
    assert booking.protection_price == 350
    assert booking.status == BookingStatus.pending_payment
    assert saved_event_seat is not None
    assert saved_event_seat.booking_id == booking.id
    assert saved_event_seat.status == SeatStatus.reserved
    assert checkout.payment.total == 5150


async def test_simultaneous_checkout_returns_conflict_for_one_request(
    database_manager,
    event_with_seat,
    payment_client,
    protection_client,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Два одновременных checkout одного места создают одну бронь и один конфликт."""
    event, _, event_seat = event_with_seat
    service = EventService(database_manager, 15, payment_client, protection_client)

    results = await asyncio.gather(
        prepare_checkout(event.id, BookingCreate(seat_ids=[event_seat.seat_id]), 1, service),
        prepare_checkout(event.id, BookingCreate(seat_ids=[event_seat.seat_id]), 2, service),
        return_exceptions=True,
    )

    async with session_factory() as session:
        bookings = (await session.scalars(select(Booking))).all()
        saved_event_seat = await session.get(EventSeat, event_seat.id)

    assert sum(isinstance(result, CheckoutResponse) for result in results) == 1
    conflicts = [result for result in results if isinstance(result, HTTPException)]
    assert len(conflicts) == 1
    assert conflicts[0].status_code == 409
    assert len(bookings) == 1
    assert saved_event_seat is not None
    assert saved_event_seat.booking_id == bookings[0].id
    assert saved_event_seat.status == SeatStatus.reserved


async def test_payment_and_protection_start_in_parallel(
    database_manager,
    event_with_seat,
    payment_client,
    protection_client,
) -> None:
    """Расчеты платежа и защиты стартуют до завершения любого из них."""
    payment_started = asyncio.Event()
    protection_started = asyncio.Event()
    release = asyncio.Event()

    async def calculate_payment(booking_id: int, amount: int) -> PaymentCalculationDTO:
        payment_started.set()
        await release.wait()
        return PaymentCalculationDTO(commission=150, total=amount + 150, payment_methods=["bank_card"])

    async def calculate_protection(
        booking_id: int,
        ticket_amount: int,
        event_category: str,
        event_starts_at: str,
    ) -> ProtectionCalculationDTO:
        protection_started.set()
        await release.wait()
        return ProtectionCalculationDTO(available=True, price=350, covered_amount=ticket_amount)

    event, _, event_seat = event_with_seat
    payment_client.calculate.side_effect = calculate_payment
    protection_client.calculate.side_effect = calculate_protection
    service = EventService(database_manager, 15, payment_client, protection_client)
    checkout_task = asyncio.create_task(service.create_checkout_booking(event.id, 1, [event_seat.seat_id]))

    await asyncio.wait_for(asyncio.gather(payment_started.wait(), protection_started.wait()), timeout=0.1)
    release.set()
    await checkout_task


async def test_payment_error_cancels_booking_and_releases_seat(
    database_manager,
    event_with_seat,
    payment_client,
    protection_client,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Ошибка платежного сервиса отменяет бронь и освобождает место."""
    event, _, event_seat = event_with_seat
    payment_client.calculate.side_effect = PaymentCalculationError
    service = EventService(database_manager, 15, payment_client, protection_client)

    with pytest.raises(PaymentCalculationError):
        await service.create_checkout_booking(event.id, 1, [event_seat.seat_id])

    async with session_factory() as session:
        bookings = (await session.scalars(select(Booking))).all()
        saved_event_seat = await session.get(EventSeat, event_seat.id)

    assert len(bookings) == 1
    assert saved_event_seat is not None
    assert bookings[0].status == BookingStatus.cancelled
    assert bookings[0].payment_commission == 0
    assert bookings[0].protection_price is None
    assert saved_event_seat.status == SeatStatus.available
    assert saved_event_seat.booking_id is None
    assert saved_event_seat.reserved_until is None


async def test_payment_error_cancels_protection_calculation(
    database_manager,
    event_with_seat,
    payment_client,
    protection_client,
) -> None:
    """Ошибка платежного сервиса отменяет незавершенный расчет защиты."""
    protection_cancelled = asyncio.Event()
    payment_client.calculate.side_effect = PaymentCalculationError

    async def calculate_protection(*_) -> ProtectionCalculationDTO:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            protection_cancelled.set()
            raise

    protection_client.calculate.side_effect = calculate_protection
    event, _, event_seat = event_with_seat
    service = EventService(database_manager, 15, payment_client, protection_client)

    with pytest.raises(PaymentCalculationError):
        await service.create_checkout_booking(event.id, 1, [event_seat.seat_id])

    assert protection_cancelled.is_set()
