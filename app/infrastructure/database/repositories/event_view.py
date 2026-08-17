from collections.abc import Mapping

from sqlalchemy.dialects.postgresql import insert

from app.infrastructure.database.models import EventView
from app.infrastructure.database.repositories.base import BaseRepository


class EventViewRepository(BaseRepository):
    """Пакетно увеличивает счётчики просмотров мероприятий."""

    async def increment_many(self, counts: Mapping[int, int]) -> None:
        """Атомарно прибавляет накопленные просмотры по мероприятиям."""
        if not counts:
            return

        statement = insert(EventView).values(
            [{"event_id": event_id, "views_count": views_count} for event_id, views_count in counts.items()]
        )
        statement = statement.on_conflict_do_update(
            index_elements=[EventView.event_id],
            set_={"views_count": EventView.views_count + statement.excluded.views_count},
        )
        await self._session.execute(statement)
