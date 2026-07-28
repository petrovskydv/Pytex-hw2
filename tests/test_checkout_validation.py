import unittest

from app.domain.exceptions import EventNotFoundError, SeatsNotFoundError
from app.services.events import EventService


class CheckoutValidationTests(unittest.IsolatedAsyncioTestCase):
    async def test_rejects_missing_event(self) -> None:
        service = EventService(None, _EventRepository(False), _EventSeatRepository(set()), 15)

        with self.assertRaises(EventNotFoundError):
            await service.create_checkout_booking(1, 1, [1])

    async def test_rejects_missing_event_seat(self) -> None:
        service = EventService(None, _EventRepository(True), _EventSeatRepository({1}), 15)

        with self.assertRaises(SeatsNotFoundError):
            await service.create_checkout_booking(1, 1, [1, 2])


class _EventRepository:
    def __init__(self, exists: bool) -> None:
        self._exists = exists

    async def exists(self, event_id: int) -> bool:
        return self._exists


class _EventSeatRepository:
    def __init__(self, seat_ids: set[int]) -> None:
        self._seat_ids = seat_ids

    async def get_existing_seat_ids(self, event_id: int, seat_ids: list[int]) -> set[int]:
        return self._seat_ids
