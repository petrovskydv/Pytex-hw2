from datetime import datetime

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.dto import EventSeatDTO
from app.domain.statuses import SeatStatus
from app.infrastructure.database.models import EventSeat


class EventSeatRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_available_for_update(self, event_id: int, seat_ids: list[int]) -> list[EventSeatDTO]:
        statement = (
            select(EventSeat)
            .where(
                EventSeat.event_id == event_id,
                EventSeat.seat_id.in_(seat_ids),
                EventSeat.status == SeatStatus.available,
            )
            .with_for_update()
        )
        event_seats = (await self._session.scalars(statement)).all()
        return [EventSeatDTO.model_validate(event_seat) for event_seat in event_seats]

    async def reserve(self, event_seat_ids: list[int], booking_id: int, reserved_until: datetime) -> None:
        statement = (
            update(EventSeat)
            .where(EventSeat.id.in_(event_seat_ids))
            .values(
                status=SeatStatus.reserved,
                booking_id=booking_id,
                reserved_until=reserved_until,
            )
        )
        await self._session.execute(statement)
