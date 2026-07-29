from typing import Annotated

from fastapi import Depends, Header, Request

from app.config import settings
from app.infrastructure.database.db import DatabaseManager, get_database_manager
from app.infrastructure.payment import PaymentClient
from app.infrastructure.protection import ProtectionClient
from app.services.events import EventService


def get_current_user_id(x_user_id: Annotated[int, Header()]) -> int:
    return x_user_id


CurrentUserId = Annotated[int, Depends(get_current_user_id)]


Database = Annotated[DatabaseManager, Depends(get_database_manager)]


def get_payment_client(request: Request) -> PaymentClient:
    return request.app.state.payment_client


def get_protection_client(request: Request) -> ProtectionClient:
    return request.app.state.protection_client


PaymentDeps = Annotated[PaymentClient, Depends(get_payment_client)]
ProtectionDeps = Annotated[ProtectionClient, Depends(get_protection_client)]


def get_event_service(
    database: Database,
    payment_client: PaymentDeps,
    protection_client: ProtectionDeps,
) -> EventService:
    return EventService(
        database,
        settings.booking_ttl_minutes,
        payment_client,
        protection_client,
    )


EventServiceDeps = Annotated[EventService, Depends(get_event_service)]
