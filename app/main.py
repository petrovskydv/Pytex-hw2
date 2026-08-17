import asyncio
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from redis import asyncio as aioredis

from app.api.routes import bookings, events, locations, organizer
from app.config import settings
from app.infrastructure.api_clients.payment import PaymentClient
from app.infrastructure.api_clients.protection import ProtectionClient
from app.infrastructure.database.add_event_data import add_event_data_to_db
from app.infrastructure.database.db import session_factory
from app.services.event_views import EVENT_VIEW_QUEUE_MAX_SIZE, EventViewService, EventViewWorker


@asynccontextmanager
async def lifespan(app: FastAPI):
    redis = aioredis.from_url(
        str(settings.redis.url),
        socket_connect_timeout=settings.redis.socket_timeout_seconds,
        socket_timeout=settings.redis.socket_timeout_seconds,
    )
    http_client = httpx.AsyncClient()
    event_view_worker_task: asyncio.Task[None] | None = None
    event_view_queue: asyncio.Queue[int | None] | None = None
    try:
        await redis.ping()
        app.state.payment_client = PaymentClient(http_client, str(settings.external_apis.payment_api_url))
        app.state.protection_client = ProtectionClient(http_client, str(settings.external_apis.protection_api_url))
        app.state.redis = redis
        await add_event_data_to_db()
        event_view_queue = asyncio.Queue(maxsize=EVENT_VIEW_QUEUE_MAX_SIZE)
        app.state.event_view_service = EventViewService(redis, event_view_queue)
        event_view_worker_task = asyncio.create_task(EventViewWorker(event_view_queue, session_factory).run())
        yield
    finally:
        try:
            if event_view_worker_task and event_view_queue:
                await event_view_queue.put(None)
                await event_view_worker_task
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
