from contextlib import asynccontextmanager
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

from app.domain.dto import PendingProtectionDTO, ProtectionCalculationDTO
from app.services.booking_cleanup import BookingCleanupService
from app.services.protection_retry import ProtectionRetryService


def _database_manager(bookings, event_seats=None):
    database = SimpleNamespace(bookings=bookings, event_seats=event_seats)

    @asynccontextmanager
    async def transaction():
        yield database

    return SimpleNamespace(transaction=transaction)


async def test_cleanup_service_keeps_release_and_delete_in_one_transaction() -> None:
    now = datetime(2030, 1, 1)
    bookings = SimpleNamespace(
        get_expired_pending_for_update=AsyncMock(return_value=[10, 11]),
        delete_expired_pending=AsyncMock(return_value=2),
    )
    event_seats = SimpleNamespace(release_reservations=AsyncMock())

    deleted = await BookingCleanupService(_database_manager(bookings, event_seats)).cleanup_expired(now)

    assert deleted == 2
    bookings.get_expired_pending_for_update.assert_awaited_once_with(now)
    event_seats.release_reservations.assert_awaited_once_with([10, 11])
    bookings.delete_expired_pending.assert_awaited_once_with([10, 11], now)


async def test_protection_service_checks_status_before_external_request() -> None:
    bookings = SimpleNamespace(
        get_pending_protection=AsyncMock(return_value=None),
        save_protection_if_pending=AsyncMock(),
    )
    protection_client = SimpleNamespace(calculate_once=AsyncMock())

    result = await ProtectionRetryService(
        _database_manager(bookings),
        protection_client,
    ).calculate_for_pending_booking(7)

    assert result is False
    protection_client.calculate_once.assert_not_awaited()
    bookings.save_protection_if_pending.assert_not_awaited()


async def test_protection_service_saves_result_conditionally() -> None:
    booking = PendingProtectionDTO(
        booking_id=7,
        amount=5000,
        event_category="conference",
        event_starts_at=datetime(2030, 1, 1),
    )
    bookings = SimpleNamespace(
        get_pending_protection=AsyncMock(return_value=booking),
        save_protection_if_pending=AsyncMock(return_value=True),
    )
    protection_client = SimpleNamespace(
        calculate_once=AsyncMock(return_value=ProtectionCalculationDTO(available=True, price=350, covered_amount=5000))
    )

    result = await ProtectionRetryService(
        _database_manager(bookings),
        protection_client,
    ).calculate_for_pending_booking(7)

    assert result is True
    protection_client.calculate_once.assert_awaited_once_with(7, 5000, "conference", "2030-01-01T00:00:00")
    bookings.save_protection_if_pending.assert_awaited_once_with(7, 350)
