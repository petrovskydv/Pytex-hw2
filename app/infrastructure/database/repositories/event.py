from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.dto import CheckoutEventDTO, DashboardEventDTO, EventReadDTO
from app.infrastructure.database.models import Event


class EventRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_id(self, event_id: int) -> CheckoutEventDTO | None:
        statement = select(Event).where(Event.id == event_id)
        event = await self._session.scalar(statement)
        return CheckoutEventDTO.model_validate(event) if event else None

    async def get_read_by_id(self, event_id: int) -> EventReadDTO | None:
        statement = select(Event).where(Event.id == event_id)
        event = await self._session.scalar(statement)
        return EventReadDTO.model_validate(event) if event else None

    async def get_dashboard_event(self, event_id: int, organizer_id: int) -> DashboardEventDTO | None:
        statement = select(Event).where(Event.id == event_id, Event.organizer_id == organizer_id)
        event = await self._session.scalar(statement)
        return DashboardEventDTO.model_validate(event) if event else None
