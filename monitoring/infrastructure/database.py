from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


class MonitoringDatabase:
    """Управляет собственным пулом подключений monitoring-сервиса к PostgreSQL."""

    def __init__(self, database_url: str) -> None:
        self.engine: AsyncEngine = create_async_engine(database_url, pool_pre_ping=True)

    async def start(self) -> None:
        """Проверяет доступность PostgreSQL до начала обработки запросов."""
        async with self.engine.connect() as connection:
            await connection.execute(text("SELECT 1"))

    async def stop(self) -> None:
        await self.engine.dispose()
