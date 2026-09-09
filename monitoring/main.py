import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from functools import partial
from typing import Any

from fastapi import FastAPI

from monitoring.api.routes import router
from monitoring.config import MonitoringSettings, get_settings
from monitoring.domain.dto import PaymentActivityAggregate
from monitoring.infrastructure.database import MonitoringDatabase
from monitoring.infrastructure.kafka import MonitoringKafka
from monitoring.infrastructure.repositories import PaymentActivityRepository
from monitoring.services.purchase_batches import process_purchase_batch

SettingsFactory = Callable[[], MonitoringSettings]
DatabaseFactory = Callable[[str], Any]
KafkaFactory = Callable[[Any, Any, Any], Any]


@asynccontextmanager
async def lifespan(
    app: FastAPI,
    settings_factory: SettingsFactory = get_settings,
    database_factory: DatabaseFactory = MonitoringDatabase,
    kafka_factory: KafkaFactory = MonitoringKafka,
) -> AsyncIterator[None]:
    settings = settings_factory()
    database = database_factory(str(settings.database.url))
    repository = PaymentActivityRepository(database.session_factory)
    batch_handler = partial(process_purchase_batch, save_aggregates=repository.save_batch)
    payment_activity_queue: asyncio.Queue[list[PaymentActivityAggregate]] = asyncio.Queue()
    kafka = kafka_factory(settings.kafka, batch_handler, payment_activity_queue)

    try:
        await database.start()
        await kafka.start()
        app.state.database = database
        app.state.kafka = kafka
        app.state.payment_activity_queue = payment_activity_queue
        yield
    finally:
        try:
            await kafka.stop()
        finally:
            await database.stop()


def create_app(
    settings_factory: SettingsFactory = get_settings,
    database_factory: DatabaseFactory = MonitoringDatabase,
    kafka_factory: KafkaFactory = MonitoringKafka,
) -> FastAPI:
    lifespan_context = partial(
        lifespan,
        settings_factory=settings_factory,
        database_factory=database_factory,
        kafka_factory=kafka_factory,
    )
    application = FastAPI(title="Afisha Purchase Monitoring", lifespan=lifespan_context)
    application.include_router(router)
    return application


app = create_app()
