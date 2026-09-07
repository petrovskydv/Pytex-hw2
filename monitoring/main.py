from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from functools import partial
from typing import Any

from fastapi import FastAPI

from monitoring.api.routes import router
from monitoring.config import MonitoringSettings, get_settings
from monitoring.infrastructure.database import MonitoringDatabase
from monitoring.infrastructure.kafka import MonitoringKafka
from monitoring.services.purchase_batches import log_purchase_batch

SettingsFactory = Callable[[], MonitoringSettings]
DatabaseFactory = Callable[[str], Any]
KafkaFactory = Callable[[Any, Any], Any]


@asynccontextmanager
async def lifespan(
    app: FastAPI,
    settings_factory: SettingsFactory = get_settings,
    database_factory: DatabaseFactory = MonitoringDatabase,
    kafka_factory: KafkaFactory = MonitoringKafka,
) -> AsyncIterator[None]:
    settings = settings_factory()
    database = database_factory(str(settings.database.url))
    kafka = kafka_factory(settings.kafka, log_purchase_batch)

    try:
        await database.start()
        await kafka.start()
        app.state.database = database
        app.state.kafka = kafka
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
