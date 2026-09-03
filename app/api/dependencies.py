from typing import Annotated

from fastapi import Depends, Header, Request
from redis.asyncio import Redis

from app.background.dispatchers import DashboardReportDispatcher, ProtectionRetryDispatcher
from app.config import settings
from app.infrastructure.api_clients.payment import PaymentClient
from app.infrastructure.api_clients.protection import ProtectionClient
from app.infrastructure.cache.event_cache import EventCache
from app.infrastructure.database.db import DatabaseManager, get_database_manager
from app.services.checkout import CheckoutService
from app.services.dashboard import DashboardService
from app.services.event_read import EventReadService
from app.services.event_views import EventViewQueue


# Простые зависимости: заголовки, БД и ресурсы приложения.
def get_current_user_id(x_user_id: Annotated[int, Header()]) -> int:
    return x_user_id


def get_payment_client(request: Request) -> PaymentClient:
    return request.app.state.payment_client


def get_protection_client(request: Request) -> ProtectionClient:
    return request.app.state.protection_client


def get_redis(request: Request) -> Redis:
    return request.app.state.redis


def get_event_view_queue(request: Request) -> EventViewQueue:
    return request.app.state.event_view_queue


def get_protection_retry_dispatcher() -> ProtectionRetryDispatcher:
    return ProtectionRetryDispatcher()


def get_dashboard_report_dispatcher() -> DashboardReportDispatcher:
    return DashboardReportDispatcher()


# Базовые алиасы зависимостей.
CurrentUserId = Annotated[int, Depends(get_current_user_id)]
Database = Annotated[DatabaseManager, Depends(get_database_manager)]


# Алиасы ресурсов приложения.
PaymentDeps = Annotated[PaymentClient, Depends(get_payment_client)]
ProtectionDeps = Annotated[ProtectionClient, Depends(get_protection_client)]
RedisDeps = Annotated[Redis, Depends(get_redis)]
EventViewQueueDeps = Annotated[EventViewQueue, Depends(get_event_view_queue)]


# Алиасы фоновых задач.
ProtectionRetryDispatcherDeps = Annotated[
    ProtectionRetryDispatcher,
    Depends(get_protection_retry_dispatcher),
]
DashboardReportDispatcherDeps = Annotated[
    DashboardReportDispatcher,
    Depends(get_dashboard_report_dispatcher),
]


# Фабрики use-case сервисов.
def get_checkout_service(
    database: Database,
    payment_client: PaymentDeps,
    protection_client: ProtectionDeps,
    protection_retry_dispatcher: ProtectionRetryDispatcherDeps,
) -> CheckoutService:
    return CheckoutService(
        database,
        settings.booking.booking_ttl_minutes,
        payment_client,
        protection_client,
        protection_retry_dispatcher,
    )


def get_event_read_service(
    database: Database,
    redis: RedisDeps,
    event_view_queue: EventViewQueueDeps,
) -> EventReadService:
    return EventReadService(
        database,
        EventCache(
            redis,
            settings.event_read.cache_ttl_seconds,
            settings.event_read.lock_ttl_seconds,
        ),
        event_view_queue,
        settings.event_read.database_timeout_seconds,
        settings.event_read.lock_wait_seconds,
    )


def get_dashboard_service(
    database: Database,
) -> DashboardService:
    return DashboardService(database)


# Алиасы use-case сервисов для маршрутов.
CheckoutServiceDeps = Annotated[CheckoutService, Depends(get_checkout_service)]
EventReadServiceDeps = Annotated[EventReadService, Depends(get_event_read_service)]
DashboardServiceDeps = Annotated[DashboardService, Depends(get_dashboard_service)]
