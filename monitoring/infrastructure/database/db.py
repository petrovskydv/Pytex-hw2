from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from monitoring.config import get_settings

settings = get_settings()

engine = create_async_engine(str(settings.database.url), pool_pre_ping=True)
session_factory = async_sessionmaker[AsyncSession](engine, expire_on_commit=False)
