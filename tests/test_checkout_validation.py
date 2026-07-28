import pytest

from app.domain.exceptions import EventNotFoundError, SeatsNotFoundError
from app.services.events import EventService


async def test_rejects_missing_event(checkout_database_manager) -> None:
    service = EventService(checkout_database_manager(False, set()), 15)

    with pytest.raises(EventNotFoundError):
        await service.create_checkout_booking(1, 1, [1])


async def test_rejects_missing_event_seat(checkout_database_manager) -> None:
    service = EventService(checkout_database_manager(True, {1}), 15)

    with pytest.raises(SeatsNotFoundError):
        await service.create_checkout_booking(1, 1, [1, 2])


async def test_transaction_commits_on_success(database_with_session) -> None:
    database, session = database_with_session

    async with database.transaction():
        pass

    assert session.commits == 1
    assert session.rollbacks == 0


async def test_transaction_rolls_back_on_error(database_with_session) -> None:
    database, session = database_with_session

    with pytest.raises(RuntimeError):
        async with database.transaction():
            raise RuntimeError

    assert session.commits == 0
    assert session.rollbacks == 1
