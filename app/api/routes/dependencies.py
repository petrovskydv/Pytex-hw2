from typing import Annotated

from fastapi import Depends, Header
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.infrastructure.database.db import get_session
from app.repositories.booking import BookingRepository
from app.repositories.event_seat import EventSeatRepository
from app.services.events import EventService


def get_current_user_id(x_user_id: Annotated[int, Header()]) -> int:
    return x_user_id


CurrentUserId = Annotated[int, Depends(get_current_user_id)]


Session = Annotated[AsyncSession, Depends(get_session)]


def get_event_seat_repository(session: Session) -> EventSeatRepository:
    return EventSeatRepository(session)


EventSeatRepositoryDependency = Annotated[EventSeatRepository, Depends(get_event_seat_repository)]


def get_booking_repository(session: Session) -> BookingRepository:
    return BookingRepository(session)


BookingRepositoryDependency = Annotated[BookingRepository, Depends(get_booking_repository)]


def get_booking_ttl_minutes() -> int:
    return settings.booking_ttl_minutes


BookingTtlMinutes = Annotated[int, Depends(get_booking_ttl_minutes)]


def get_event_service(
    booking_repository: BookingRepositoryDependency,
    event_seat_repository: EventSeatRepositoryDependency,
    booking_ttl_minutes: BookingTtlMinutes,
) -> EventService:
    return EventService(booking_repository, event_seat_repository, booking_ttl_minutes)


CurrentEventService = Annotated[EventService, Depends(get_event_service)]
