from typing import Annotated

from fastapi import Depends, Header

from app.config import settings
from app.infrastructure.database.db import DatabaseManager, get_database_manager
from app.services.events import EventService


def get_current_user_id(x_user_id: Annotated[int, Header()]) -> int:
    return x_user_id


CurrentUserId = Annotated[int, Depends(get_current_user_id)]


Database = Annotated[DatabaseManager, Depends(get_database_manager)]


def get_event_service(database: Database) -> EventService:
    return EventService(database, settings.booking_ttl_minutes)


CurrentEventService = Annotated[EventService, Depends(get_event_service)]
