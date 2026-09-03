from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from redis import asyncio as aioredis

from app.api.routes import bookings, events, locations, organizer
from app.background.brokers import insurance_broker, reports_broker
from app.config import settings
from app.infrastructure.api_clients.payment import PaymentClient
from app.infrastructure.api_clients.protection import ProtectionClient
from app.infrastructure.database.add_event_data import add_event_data_to_db
from app.infrastructure.database.db import session_factory
from app.services.event_views import EventViewQueue


@asynccontextmanager
async def lifespan(app: FastAPI):
    redis = aioredis.from_url(
        str(settings.redis.url),
        socket_connect_timeout=settings.redis.socket_timeout_seconds,
        socket_timeout=settings.redis.socket_timeout_seconds,
    )
    http_client = httpx.AsyncClient()
    event_view_queue: EventViewQueue | None = None
    try:
        app.state.payment_client = PaymentClient(http_client, settings.external_apis.payment_api_url)
        app.state.protection_client = ProtectionClient(http_client, settings.external_apis.protection_api_url)

        # заполнение бд тестовыми данными
        await add_event_data_to_db()

        await redis.ping()
        await reports_broker.startup()
        await insurance_broker.startup()
        app.state.redis = redis
        event_view_queue = EventViewQueue(
            redis,
            session_factory,
            batch_size=settings.event_view.batch_size,
            flush_seconds=settings.event_view.flush_seconds,
        )
        event_view_queue.start()
        app.state.event_view_queue = event_view_queue
        yield
    finally:
        try:
            if event_view_queue:
                await event_view_queue.stop()
        finally:
            try:
                await reports_broker.shutdown()
                await insurance_broker.shutdown()
            finally:
                await http_client.aclose()
                await redis.aclose()


app = FastAPI(title="API Афиши", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(locations.router)
app.include_router(events.router)
app.include_router(organizer.router)
app.include_router(bookings.router)
