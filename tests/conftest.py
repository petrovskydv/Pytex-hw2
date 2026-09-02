import asyncio
import os
import subprocess
import sys
from datetime import datetime
from unittest.mock import AsyncMock, create_autospec

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool
from testcontainers.community.postgres import PostgresContainer

from app.domain.dto import PaymentCalculationDTO, ProtectionCalculationDTO
from app.infrastructure.api_clients.payment import PaymentClient
from app.infrastructure.api_clients.protection import ProtectionClient
from app.infrastructure.database.db import DatabaseManager
from app.infrastructure.database.models import Event, EventSeat, Location, Seat

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


@pytest.fixture(scope="session")
def postgres_url() -> str:
    container = PostgresContainer("postgres:17-alpine")
    container.start()
    try:
        url = container.get_connection_url().replace("postgresql+psycopg2://", "postgresql+psycopg://")
        subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head"],
            check=True,
            env={**os.environ, "DATABASE_URL": url, "DATABASE__URL": url},
        )
        yield url
    finally:
        container.stop()


@pytest.fixture(scope="session")
def engine(postgres_url: str) -> AsyncEngine:
    engine = create_async_engine(postgres_url, poolclass=NullPool)
    yield engine
    asyncio.run(engine.dispose())


@pytest.fixture
async def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    async with engine.begin() as connection:
        await connection.execute(
            text("TRUNCATE event_views, event_seats, bookings, events, seats, locations RESTART IDENTITY CASCADE")
        )
    return async_sessionmaker(engine, expire_on_commit=False)


@pytest.fixture
async def database_manager(session_factory: async_sessionmaker[AsyncSession]) -> DatabaseManager:
    async with session_factory() as session:
        yield DatabaseManager(session, session_factory)


@pytest.fixture
async def event_with_seat(session_factory: async_sessionmaker[AsyncSession]) -> tuple[Event, Seat, EventSeat]:
    async with session_factory() as session:
        location = Location(name="Test location", city="Moscow", address="Test address")
        session.add(location)
        await session.flush()

        seat = Seat(location_id=location.id, sector="A", row=1, number=1, x=1, y=1)
        session.add(seat)
        await session.flush()

        event = Event(
            organizer_id=1,
            location_id=location.id,
            title="Test event",
            description=None,
            category="conference",
            starts_at=datetime(2030, 1, 1),
            base_price=5000,
        )
        session.add(event)
        await session.flush()

        event_seat = EventSeat(event_id=event.id, seat_id=seat.id, price=5000)
        session.add(event_seat)
        await session.commit()
        return event, seat, event_seat


@pytest.fixture
def payment_client():
    client = create_autospec(PaymentClient, instance=True)
    client.calculate.return_value = PaymentCalculationDTO(
        commission=150,
        total=5150,
        payment_methods=["bank_card"],
    )
    return client


@pytest.fixture
def protection_client():
    client = create_autospec(ProtectionClient, instance=True)
    client.calculate.return_value = ProtectionCalculationDTO(
        available=True,
        price=350,
        covered_amount=5000,
    )
    return client


@pytest.fixture
def protection_retry_dispatcher():
    dispatcher = AsyncMock()
    dispatcher.enqueue = AsyncMock()
    return dispatcher


@pytest.fixture
def dashboard_report_dispatcher():
    dispatcher = AsyncMock()
    dispatcher.enqueue = AsyncMock()
    return dispatcher
