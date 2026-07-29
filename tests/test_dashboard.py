import asyncio
from datetime import UTC, datetime

import pytest
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.routes.organizer import get_event_dashboard
from app.domain.dto import OccupancyDashboardDTO, SalesDashboardDTO
from app.domain.exceptions import EventNotFoundError
from app.domain.statuses import BookingStatus, SeatStatus
from app.infrastructure.database.models import Booking, Event, EventSeat, Seat
from app.repositories.booking import BookingRepository
from app.repositories.event_seat import EventSeatRepository
from app.services.dashboard import DashboardService


async def test_dashboard_aggregates_sales_and_occupancy(
    database_manager,
    event_with_seat,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Дашборд считает оплаченные брони и места каждого статуса."""
    event, seat, event_seat = event_with_seat
    async with session_factory() as session:
        paid_first = Booking(
            event_id=event.id,
            user_id=2,
            amount=1001,
            payment_commission=0,
            protection_price=None,
            with_protection=False,
            status=BookingStatus.paid,
            reserved_until=datetime.now(UTC).replace(tzinfo=None),
        )
        paid_second = Booking(
            event_id=event.id,
            user_id=3,
            amount=1002,
            payment_commission=0,
            protection_price=None,
            with_protection=False,
            status=BookingStatus.paid,
            reserved_until=datetime.now(UTC).replace(tzinfo=None),
        )
        unpaid = Booking(
            event_id=event.id,
            user_id=4,
            amount=9999,
            payment_commission=0,
            protection_price=None,
            with_protection=False,
            status=BookingStatus.pending_payment,
            reserved_until=datetime.now(UTC).replace(tzinfo=None),
        )
        session.add_all([paid_first, paid_second, unpaid])
        await session.flush()

        reserved_seat = Seat(location_id=seat.location_id, sector="A", row=1, number=2, x=2, y=1)
        sold_seat = Seat(location_id=seat.location_id, sector="A", row=1, number=3, x=3, y=1)
        session.add_all([reserved_seat, sold_seat])
        await session.flush()
        session.add_all(
            [
                EventSeat(event_id=event.id, seat_id=reserved_seat.id, price=5000, status=SeatStatus.reserved),
                EventSeat(event_id=event.id, seat_id=sold_seat.id, price=5000, status=SeatStatus.sold),
            ]
        )
        await session.commit()

    dashboard = await DashboardService(database_manager).get_dashboard(event.id, event.organizer_id)

    assert event_seat.status == SeatStatus.available
    assert dashboard.sales.paid_orders == 2
    assert dashboard.sales.sold_tickets == 1
    assert dashboard.sales.revenue == 2003
    assert dashboard.sales.average_order == 1002
    assert dashboard.occupancy.model_dump() == {"total": 3, "available": 1, "reserved": 1, "sold": 1}

    response = await get_event_dashboard(event.id, event.organizer_id, DashboardService(database_manager))
    assert response.occupancy.occupancy_percent == pytest.approx(66.66666666666667)


async def test_dashboard_returns_zero_for_empty_aggregates(
    database_manager,
    event_with_seat,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Дашборд мероприятия без броней и мест возвращает нулевые показатели."""
    event, _, _ = event_with_seat
    async with session_factory() as session:
        empty_event = Event(
            organizer_id=event.organizer_id,
            location_id=event.location_id,
            title="Empty event",
            description=None,
            category="conference",
            starts_at=event.starts_at,
            base_price=event.base_price,
        )
        session.add(empty_event)
        await session.commit()

    dashboard = await DashboardService(database_manager).get_dashboard(empty_event.id, empty_event.organizer_id)

    assert dashboard.sales.model_dump() == {"paid_orders": 0, "sold_tickets": 0, "revenue": 0, "average_order": 0}
    assert dashboard.occupancy.model_dump() == {"total": 0, "available": 0, "reserved": 0, "sold": 0}


async def test_dashboard_returns_not_found_for_missing_or_foreign_event(database_manager, event_with_seat) -> None:
    """Не раскрывает существование чужого мероприятия."""
    event, _, _ = event_with_seat
    service = DashboardService(database_manager)

    with pytest.raises(EventNotFoundError):
        await service.get_dashboard(event.id + 1, event.organizer_id)
    with pytest.raises(EventNotFoundError):
        await service.get_dashboard(event.id, event.organizer_id + 1)
    with pytest.raises(HTTPException, match="Event not found") as error:
        await get_event_dashboard(event.id, event.organizer_id + 1, service)

    assert error.value.status_code == 404


async def test_dashboard_runs_aggregates_in_parallel_with_different_sessions(
    database_manager,
    event_with_seat,
    monkeypatch,
) -> None:
    """Агрегаты начинают работу одновременно на отдельных сессиях."""
    sales_started = asyncio.Event()
    occupancy_started = asyncio.Event()
    release = asyncio.Event()
    sessions = []

    async def get_sales(self, event_id):
        sessions.append(self._session)
        sales_started.set()
        await release.wait()
        return SalesDashboardDTO(paid_orders=0, sold_tickets=0, revenue=0, average_order=0)

    async def get_occupancy(self, event_id):
        sessions.append(self._session)
        occupancy_started.set()
        await release.wait()
        return OccupancyDashboardDTO(total=0, available=0, reserved=0, sold=0)

    monkeypatch.setattr(BookingRepository, "get_sales_dashboard", get_sales)
    monkeypatch.setattr(EventSeatRepository, "get_occupancy_dashboard", get_occupancy)
    event, _, _ = event_with_seat
    task = asyncio.create_task(DashboardService(database_manager).get_dashboard(event.id, event.organizer_id))

    await asyncio.wait_for(asyncio.gather(sales_started.wait(), occupancy_started.wait()), timeout=0.1)
    release.set()
    await task

    assert sessions[0] is not sessions[1]
