import asyncio
from datetime import UTC, datetime, timedelta

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from taskiq.middlewares import SmartRetryMiddleware

from app.background.brokers import (
    CLEANUP_QUEUE,
    INSURANCE_QUEUE,
    REPORTS_QUEUE,
    insurance_broker,
    retry_schedule_source,
)
from app.background.jobs import (
    build_dashboard_report,
    cleanup_expired_bookings_task,
    generate_dashboard_report_task,
    retry_protection_task,
)
from app.domain.statuses import BookingStatus, SeatStatus
from app.infrastructure.database.db import DatabaseManager
from app.infrastructure.database.models import Booking, EventSeat
from app.services.booking_cleanup import BookingCleanupService
from app.services.protection_retry import ProtectionRetryService


def test_taskiq_routing_schedule_and_retry_contract() -> None:
    """TaskIQ разделяет очереди, запускает cleanup раз в минуту и делает один retry."""
    assert generate_dashboard_report_task.labels["queue_name"] == REPORTS_QUEUE
    assert cleanup_expired_bookings_task.labels == {
        "queue_name": CLEANUP_QUEUE,
        "ack_type": "when_executed",
        "schedule": [{"cron": "* * * * *"}],
    }
    assert retry_protection_task.labels["queue_name"] == INSURANCE_QUEUE
    assert retry_protection_task.labels["ack_type"] == "when_executed"
    assert retry_protection_task.labels["retry_on_error"] is True
    assert retry_protection_task.labels["max_retries"] == 2
    retry_middleware = next(
        middleware for middleware in insurance_broker.middlewares if isinstance(middleware, SmartRetryMiddleware)
    )
    assert retry_middleware.default_retry_count == 2
    assert retry_middleware.schedule_source is retry_schedule_source


def test_build_dashboard_report_creates_only_complete_pdf(tmp_path) -> None:
    dashboard = {
        "event_title": "Python Conference",
        "starts_at": "2030-01-01T12:00:00",
        "sales": {"paid_orders": 2, "sold_tickets": 3, "revenue": 15000, "average_order": 7500},
        "occupancy": {"total": 10, "available": 6, "reserved": 1, "sold": 3},
    }

    output_path = build_dashboard_report(42, dashboard, tmp_path)

    assert output_path.parent == tmp_path
    assert output_path.name.startswith("dashboard-event-42-")
    assert output_path.read_bytes().startswith(b"%PDF")
    assert list(tmp_path.glob("*.tmp")) == []


async def test_cleanup_deletes_expired_pending_and_releases_seat(
    database_manager,
    event_with_seat,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event, _, event_seat = event_with_seat
    now = datetime.now(UTC).replace(tzinfo=None)
    async with session_factory() as session:
        booking = Booking(
            event_id=event.id,
            user_id=1,
            amount=event_seat.price,
            payment_commission=0,
            protection_price=None,
            with_protection=False,
            status=BookingStatus.pending_payment,
            reserved_until=now - timedelta(seconds=1),
        )
        session.add(booking)
        await session.flush()
        saved_seat = await session.get(EventSeat, event_seat.id)
        saved_seat.status = SeatStatus.reserved
        saved_seat.booking_id = booking.id
        saved_seat.reserved_until = booking.reserved_until
        await session.commit()
        booking_id = booking.id

    deleted_count = await BookingCleanupService(database_manager).cleanup_expired(now)

    async with session_factory() as session:
        saved_booking = await session.get(Booking, booking_id)
        saved_seat = await session.get(EventSeat, event_seat.id)
    assert deleted_count == 1
    assert saved_booking is None
    assert saved_seat.status == SeatStatus.available
    assert saved_seat.booking_id is None
    assert saved_seat.reserved_until is None


async def test_cleanup_keeps_active_and_paid_bookings(
    database_manager,
    event_with_seat,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event, _, _ = event_with_seat
    now = datetime.now(UTC).replace(tzinfo=None)
    async with session_factory() as session:
        active = Booking(
            event_id=event.id,
            user_id=1,
            amount=5000,
            payment_commission=0,
            protection_price=None,
            with_protection=False,
            status=BookingStatus.pending_payment,
            reserved_until=now + timedelta(minutes=1),
        )
        paid = Booking(
            event_id=event.id,
            user_id=2,
            amount=5000,
            payment_commission=0,
            protection_price=None,
            with_protection=False,
            status=BookingStatus.paid,
            reserved_until=now - timedelta(minutes=1),
        )
        session.add_all([active, paid])
        await session.commit()
        booking_ids = (active.id, paid.id)

    deleted_count = await BookingCleanupService(database_manager).cleanup_expired(now)

    async with session_factory() as session:
        saved = [await session.get(Booking, booking_id) for booking_id in booking_ids]
    assert deleted_count == 0
    assert all(booking is not None for booking in saved)


async def test_concurrent_cleanup_processes_booking_once(
    event_with_seat,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event, _, event_seat = event_with_seat
    now = datetime.now(UTC).replace(tzinfo=None)
    async with session_factory() as session:
        booking = Booking(
            event_id=event.id,
            user_id=1,
            amount=event_seat.price,
            payment_commission=0,
            protection_price=None,
            with_protection=False,
            status=BookingStatus.pending_payment,
            reserved_until=now - timedelta(seconds=1),
        )
        session.add(booking)
        await session.flush()
        saved_seat = await session.get(EventSeat, event_seat.id)
        saved_seat.status = SeatStatus.reserved
        saved_seat.booking_id = booking.id
        saved_seat.reserved_until = booking.reserved_until
        await session.commit()

    async with session_factory() as first, session_factory() as second:
        services = (
            BookingCleanupService(DatabaseManager(first, session_factory)),
            BookingCleanupService(DatabaseManager(second, session_factory)),
        )
        results = await asyncio.gather(*(service.cleanup_expired(now) for service in services))

    assert sorted(results) == [0, 1]


async def test_protection_retry_saves_once_for_pending_booking(
    database_manager,
    event_with_seat,
    protection_client,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event, _, _ = event_with_seat
    async with session_factory() as session:
        booking = Booking(
            event_id=event.id,
            user_id=1,
            amount=5000,
            payment_commission=150,
            protection_price=None,
            with_protection=False,
            status=BookingStatus.pending_payment,
            reserved_until=datetime.now(UTC).replace(tzinfo=None) + timedelta(minutes=1),
        )
        session.add(booking)
        await session.commit()
        booking_id = booking.id
    protection_client.calculate_once.return_value.price = 350
    protection_client.calculate_once.return_value.available = True
    service = ProtectionRetryService(database_manager, protection_client)

    assert await service.calculate_for_pending_booking(booking_id) is True
    assert await service.calculate_for_pending_booking(booking_id) is False

    async with session_factory() as session:
        saved_booking = await session.get(Booking, booking_id)
    assert saved_booking.protection_price == 350
    protection_client.calculate_once.assert_awaited_once()


async def test_protection_retry_skips_non_pending_booking(
    database_manager,
    event_with_seat,
    protection_client,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    event, _, _ = event_with_seat
    async with session_factory() as session:
        booking = Booking(
            event_id=event.id,
            user_id=1,
            amount=5000,
            payment_commission=150,
            protection_price=None,
            with_protection=False,
            status=BookingStatus.paid,
            reserved_until=datetime.now(UTC).replace(tzinfo=None),
        )
        session.add(booking)
        await session.commit()
        booking_id = booking.id

    result = await ProtectionRetryService(database_manager, protection_client).calculate_for_pending_booking(booking_id)

    assert result is False
    protection_client.calculate_once.assert_not_awaited()
