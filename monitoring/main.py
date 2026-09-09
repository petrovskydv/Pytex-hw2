import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from functools import partial
from typing import Any

from fastapi import FastAPI

from monitoring.api.routes import router
from monitoring.config import MonitoringSettings, get_settings
from monitoring.domain.dto import PaymentActivityAggregate
from monitoring.infrastructure.database import engine, session_factory
from monitoring.infrastructure.kafka import MonitoringKafka
from monitoring.infrastructure.repositories import PaymentActivityRepository
from monitoring.services.purchase_batches import process_purchase_batch
from monitoring.services.websocket_delivery import (
    PaymentActivityWebSocketWorker,
    WebSocketConnectionManager,
)

SettingsFactory = Callable[[], MonitoringSettings]
KafkaFactory = Callable[[Any, Any, Any], Any]


@asynccontextmanager
async def lifespan(
    app: FastAPI,
    settings_factory: SettingsFactory = get_settings,
    kafka_factory: KafkaFactory = MonitoringKafka,
) -> AsyncIterator[None]:
    settings = settings_factory()
    repository = PaymentActivityRepository(session_factory)
    batch_handler = partial(process_purchase_batch, save_aggregates=repository.save_batch)
    payment_activity_queue: asyncio.Queue[list[PaymentActivityAggregate]] = asyncio.Queue()
    websocket_manager = WebSocketConnectionManager()
    websocket_worker = PaymentActivityWebSocketWorker(
        payment_activity_queue,
        websocket_manager,
        settings.websocket.send_timeout_seconds,
    )
    kafka = kafka_factory(settings.kafka, batch_handler, payment_activity_queue)

    try:
        websocket_worker.start()
        await kafka.start()
        app.state.kafka = kafka
        app.state.payment_activity_queue = payment_activity_queue
        app.state.websocket_manager = websocket_manager
        app.state.websocket_worker = websocket_worker
        yield
    finally:
        try:
            await kafka.stop()
        finally:
            try:
                await websocket_worker.stop()
            finally:
                await engine.dispose()


def create_app(
    settings_factory: SettingsFactory = get_settings,
    kafka_factory: KafkaFactory = MonitoringKafka,
) -> FastAPI:
    lifespan_context = partial(
        lifespan,
        settings_factory=settings_factory,
        kafka_factory=kafka_factory,
    )
    application = FastAPI(title="Afisha Purchase Monitoring", lifespan=lifespan_context)
    application.include_router(router)
    return application


app = create_app()
