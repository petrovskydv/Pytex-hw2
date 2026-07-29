import asyncio
from datetime import datetime

import pytest
from fastapi import HTTPException

from app.api.routes.events import prepare_checkout
from app.api.schemas import BookingCreate, CheckoutResponse
from app.domain.dto import CheckoutEventDTO, PaymentCalculationDTO, ProtectionCalculationDTO
from app.domain.exceptions import EventNotFoundError, PaymentCalculationError, SeatsNotFoundError
from app.services.events import EventService


async def test_rejects_missing_event(checkout_database_manager, payment_client, protection_client) -> None:
    service = EventService(checkout_database_manager(False, set()), 15, payment_client, protection_client)

    with pytest.raises(EventNotFoundError):
        await service.create_checkout_booking(1, 1, [1])


async def test_rejects_missing_event_seat(checkout_database_manager, payment_client, protection_client) -> None:
    service = EventService(checkout_database_manager(True, {1}), 15, payment_client, protection_client)

    with pytest.raises(SeatsNotFoundError):
        await service.create_checkout_booking(1, 1, [1, 2])


async def test_transaction_commits_on_success(database_with_session) -> None:
    database, session = database_with_session

    async with database.transaction():
        pass

    assert session.commits == 1
    assert session.rollbacks == 0


async def test_transaction_rolls_back_on_error(database_with_session) -> None:
    database, session = database_with_session

    with pytest.raises(RuntimeError):
        async with database.transaction():
            raise RuntimeError

    assert session.commits == 0
    assert session.rollbacks == 1


async def test_checkout_saves_payment_and_protection_calculation(
    checkout_database_manager,
    payment_client,
    protection_client,
) -> None:
    database = checkout_database_manager(True, {1})
    service = EventService(database, 15, payment_client, protection_client)

    checkout = await service.create_checkout_booking(1, 1, [1])

    assert database.transaction_count == 1
    assert database.commit_count == 1
    assert database.bookings.calculations == [(1, 150, 350)]
    assert checkout.payment.total == 5150
    assert checkout.protection is not None


async def test_simultaneous_checkout_returns_conflict_for_one_request(
    checkout_database_manager,
    payment_client,
    protection_client,
) -> None:
    database = checkout_database_manager(True, {1})
    service = EventService(database, 15, payment_client, protection_client)

    results = await asyncio.gather(
        prepare_checkout(1, BookingCreate(seat_ids=[1]), 1, service),
        prepare_checkout(1, BookingCreate(seat_ids=[1]), 2, service),
        return_exceptions=True,
    )

    assert sum(isinstance(result, CheckoutResponse) for result in results) == 1
    conflicts = [result for result in results if isinstance(result, HTTPException)]
    assert len(conflicts) == 1
    assert conflicts[0].status_code == 409


async def test_payment_and_protection_start_in_parallel(checkout_database_manager) -> None:
    payment_started = asyncio.Event()
    protection_started = asyncio.Event()
    release = asyncio.Event()

    class BlockingPaymentClient:
        async def calculate(self, booking_id: int, amount: int) -> PaymentCalculationDTO:
            payment_started.set()
            await release.wait()
            return PaymentCalculationDTO(commission=150, total=amount + 150, payment_methods=["bank_card"])

    class BlockingProtectionClient:
        async def calculate(
            self,
            booking_id: int,
            ticket_amount: int,
            event_category: str,
            event_starts_at: str,
        ) -> ProtectionCalculationDTO:
            protection_started.set()
            await release.wait()
            return ProtectionCalculationDTO(available=True, price=350, covered_amount=ticket_amount)

    service = EventService(
        checkout_database_manager(True, {1}),
        15,
        BlockingPaymentClient(),
        BlockingProtectionClient(),
    )
    checkout_task = asyncio.create_task(service.create_checkout_booking(1, 1, [1]))

    await asyncio.wait_for(asyncio.gather(payment_started.wait(), protection_started.wait()), timeout=0.1)
    release.set()
    await checkout_task


async def test_payment_error_keeps_reserved_booking(
    checkout_database_manager,
    protection_client,
) -> None:
    class FailingPaymentClient:
        async def calculate(self, booking_id: int, amount: int) -> PaymentCalculationDTO:
            raise PaymentCalculationError

    database = checkout_database_manager(True, {1})
    service = EventService(database, 15, FailingPaymentClient(), protection_client)

    with pytest.raises(PaymentCalculationError):
        await service.create_checkout_booking(1, 1, [1])

    assert len(database.bookings.created) == 1
    assert database.event_seats._available_seat_ids == set()
    assert database.bookings.calculations == []


async def test_payment_error_cancels_protection_calculation(checkout_database_manager, monkeypatch) -> None:
    protection_cancelled = asyncio.Event()

    class FailingPaymentClient:
        async def calculate(self, booking_id: int, amount: int) -> PaymentCalculationDTO:
            raise PaymentCalculationError

    class BlockingProtectionClient:
        async def calculate(
            self,
            booking_id: int,
            ticket_amount: int,
            event_category: str,
            event_starts_at: str,
        ) -> ProtectionCalculationDTO:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                protection_cancelled.set()
                raise

    service = EventService(
        checkout_database_manager(True, {1}),
        15,
        FailingPaymentClient(),
        BlockingProtectionClient(),
    )

    async def get_event(*_) -> CheckoutEventDTO:
        return CheckoutEventDTO(
            id=1,
            title="Test event",
            category="conference",
            starts_at=datetime(2030, 1, 1),
        )

    monkeypatch.setattr(service, "_get_event", get_event)

    with pytest.raises(PaymentCalculationError):
        await service.create_checkout_booking(1, 1, [1])

    assert protection_cancelled.is_set()
