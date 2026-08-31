from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import settings
from app.infrastructure.database.repositories.booking import BookingRepository
from app.infrastructure.database.repositories.event import EventRepository
from app.infrastructure.database.repositories.event_seat import EventSeatRepository
from app.infrastructure.database.repositories.event_view import EventViewRepository

engine = create_async_engine(str(settings.database.url), pool_pre_ping=True)
session_factory = async_sessionmaker(engine, expire_on_commit=False)


class DatabaseManager:
    def __init__(
        self,
        session: AsyncSession,
        session_maker: async_sessionmaker[AsyncSession],
    ) -> None:
        self._session = session
        self._session_maker = session_maker

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator["DatabaseManager"]:
        async with self._session_maker() as session:
            database_manager = DatabaseManager(session, self._session_maker)
            try:
                yield database_manager
                await database_manager.commit()
            except Exception:
                await database_manager.rollback()
                raise

    async def commit(self) -> None:
        await self._session.commit()

    async def rollback(self) -> None:
        await self._session.rollback()

    @property
    def bookings(self) -> BookingRepository:
        return BookingRepository(self._session)

    @property
    def events(self) -> EventRepository:
        return EventRepository(self._session)

    @property
    def event_seats(self) -> EventSeatRepository:
        return EventSeatRepository(self._session)

    @property
    def event_views(self) -> EventViewRepository:
        return EventViewRepository(self._session)


async def get_database_manager() -> AsyncIterator[DatabaseManager]:
    async with session_factory() as session:
        database_manager = DatabaseManager(session, session_factory)
        try:
            yield database_manager
        except Exception:
            await database_manager.rollback()
            raise
