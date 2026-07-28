from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from app.config import settings

engine = create_async_engine(str(settings.database_url), pool_pre_ping=True)


async def get_session() -> AsyncIterator[AsyncSession]:
    async with AsyncSession(engine, expire_on_commit=False) as session, session.begin():
        yield session
