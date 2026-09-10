import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from faststream.kafka import KafkaBroker

from monitoring.api.routes import router
from monitoring.config import get_settings
from monitoring.domain.dto import PaymentActivityAggregate
from monitoring.infrastructure.database.db import engine, session_factory
from monitoring.infrastructure.database.repositories.payment_activity import PaymentActivityRepository
from monitoring.infrastructure.kafka import MonitoringKafka
from monitoring.services.purchase_batches import PurchaseBatchProcessor
from monitoring.services.websocket_delivery import (
    PaymentActivityWebSocketWorker,
    WebSocketConnectionManager,
)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    repository = PaymentActivityRepository(session_factory)
    processor = PurchaseBatchProcessor(repository)
    payment_activity_queue: asyncio.Queue[list[PaymentActivityAggregate]] = asyncio.Queue()
    websocket_manager = WebSocketConnectionManager()
    websocket_worker = PaymentActivityWebSocketWorker(
        payment_activity_queue,
        websocket_manager,
        settings.websocket.send_timeout_seconds,
    )
    kafka_broker = KafkaBroker(
        settings.kafka.bootstrap_servers,
        consumer_only=True,
    )
    kafka = MonitoringKafka(
        kafka_broker,
        settings.kafka,
        processor,
        payment_activity_queue,
    )

    try:
        websocket_worker.start()
        await kafka_broker.start()
        app.state.kafka_broker = kafka_broker
        app.state.kafka = kafka
        app.state.payment_activity_queue = payment_activity_queue
        app.state.websocket_manager = websocket_manager
        app.state.websocket_worker = websocket_worker
        yield
    finally:
        await kafka_broker.stop()
        await websocket_worker.stop()
        await engine.dispose()


app = FastAPI(title="Afisha Purchase Monitoring", lifespan=lifespan)
app.include_router(router)
