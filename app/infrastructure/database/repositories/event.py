from sqlalchemy import select

from app.domain.dto import DashboardEventDTO, EventDetailsDTO
from app.infrastructure.database.models import Event
from app.infrastructure.database.repositories.base import BaseRepository


class EventRepository(BaseRepository):
    async def get_by_id(self, event_id: int) -> EventDetailsDTO | None:
        statement = select(Event).where(Event.id == event_id)
        event = await self._session.scalar(statement)
        return EventDetailsDTO.model_validate(event) if event else None

    async def get_dashboard_event(self, event_id: int, organizer_id: int) -> DashboardEventDTO | None:
        statement = select(Event).where(Event.id == event_id, Event.organizer_id == organizer_id)
        event = await self._session.scalar(statement)
        return DashboardEventDTO.model_validate(event) if event else None
