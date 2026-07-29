from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.dto import CheckoutEventDTO
from app.infrastructure.database.models import Event


class EventRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_id(self, event_id: int) -> CheckoutEventDTO | None:
        statement = select(Event).where(Event.id == event_id)
        event = await self._session.scalar(statement)
        return CheckoutEventDTO.model_validate(event) if event else None
