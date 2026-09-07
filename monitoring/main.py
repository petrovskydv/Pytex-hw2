from collections.abc import Callable
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI

from monitoring.api.routes import router
from monitoring.config import MonitoringSettings, get_settings
from monitoring.infrastructure.database import MonitoringDatabase
from monitoring.infrastructure.kafka import MonitoringKafka

SettingsFactory = Callable[[], MonitoringSettings]
DatabaseFactory = Callable[[str], Any]
KafkaFactory = Callable[[Any], Any]


def create_app(
    settings_factory: SettingsFactory = get_settings,
    database_factory: DatabaseFactory = MonitoringDatabase,
    kafka_factory: KafkaFactory = MonitoringKafka,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        settings = settings_factory()
        database = database_factory(str(settings.database.url))
        kafka = kafka_factory(settings.kafka)

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

    application = FastAPI(title="Afisha Purchase Monitoring", lifespan=lifespan)
    application.include_router(router)
    return application


app = create_app()
