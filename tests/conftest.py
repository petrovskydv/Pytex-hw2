from collections.abc import Callable
from contextlib import asynccontextmanager

import pytest

from app.infrastructure.database.db import DatabaseManager


@pytest.fixture
def checkout_database_manager() -> Callable[[bool, set[int]], "FakeDatabaseManager"]:
    return FakeDatabaseManager


@pytest.fixture
def database_with_session() -> tuple[DatabaseManager, "FakeSession"]:
    session = FakeSession()
    return DatabaseManager(session, FakeSessionMaker(session)), session


class FakeDatabaseManager:
    def __init__(self, event_exists: bool, seat_ids: set[int]) -> None:
        self.events = FakeEventRepository(event_exists)
        self.event_seats = FakeEventSeatRepository(seat_ids)

    @asynccontextmanager
    async def transaction(self):
        yield self


class FakeEventRepository:
    def __init__(self, exists: bool) -> None:
        self._exists = exists

    async def exists(self, event_id: int) -> bool:
        return self._exists


class FakeEventSeatRepository:
    def __init__(self, seat_ids: set[int]) -> None:
        self._seat_ids = seat_ids

    async def get_existing_seat_ids(self, event_id: int, seat_ids: list[int]) -> set[int]:
        return self._seat_ids


class FakeSession:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        return None

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1


class FakeSessionMaker:
    def __init__(self, session: FakeSession) -> None:
        self._session = session

    def __call__(self) -> FakeSession:
        return self._session
