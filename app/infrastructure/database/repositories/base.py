from sqlalchemy.ext.asyncio import AsyncSession


class BaseRepository:
    """Общая база репозиториев с сессией базы данных."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
