from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.infrastructure.database.models import Event


class EventRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def exists(self, event_id: int) -> bool:
        statement = select(Event.id).where(Event.id == event_id)
        return (await self._session.scalar(statement)) is not None
