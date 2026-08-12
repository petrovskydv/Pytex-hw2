from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import bookings, events, locations, organizer
from app.config import settings
from app.infrastructure.api_clients.payment import PaymentClient
from app.infrastructure.api_clients.protection import ProtectionClient
from app.infrastructure.database.add_event_data import add_event_data_to_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with httpx.AsyncClient() as http_client:
        app.state.payment_client = PaymentClient(http_client, str(settings.payment_api_url))
        app.state.protection_client = ProtectionClient(http_client, str(settings.protection_api_url))
        await add_event_data_to_db()
        yield


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
