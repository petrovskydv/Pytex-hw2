from typing import Annotated

from fastapi import Depends, Header, Request
from redis.asyncio import Redis

from app.config import settings
from app.infrastructure.api_clients.payment import PaymentClient
from app.infrastructure.api_clients.protection import ProtectionClient
from app.infrastructure.database.db import DatabaseManager, get_database_manager
from app.services.dashboard import DashboardService
from app.services.event_read import EventReadService
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


def get_redis(request: Request) -> Redis:
    return request.app.state.redis


RedisDeps = Annotated[Redis, Depends(get_redis)]


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


def get_event_read_service(database: Database, redis: RedisDeps) -> EventReadService:
    return EventReadService(
        database,
        redis,
        settings.event_cache_ttl_seconds,
        settings.event_lock_ttl_seconds,
        settings.event_database_timeout_seconds,
        settings.event_lock_wait_seconds,
    )


EventReadServiceDeps = Annotated[EventReadService, Depends(get_event_read_service)]


def get_dashboard_service(database: Database) -> DashboardService:
    return DashboardService(database)


DashboardServiceDeps = Annotated[DashboardService, Depends(get_dashboard_service)]
