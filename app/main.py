from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.add_event_data import add_event_data_to_db
from app.routers.bookings import router as bookings_router
from app.routers.events import router as events_router
from app.routers.locations import router as locations_router
from app.routers.organizer import router as organizer_router


@asynccontextmanager
async def lifespan(app: FastAPI):
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

app.include_router(locations_router)
app.include_router(events_router)
app.include_router(organizer_router)
app.include_router(bookings_router)
