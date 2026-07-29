import asyncio
from collections.abc import Callable
from contextlib import asynccontextmanager
from datetime import datetime

import pytest

from app.domain.dto import BookingDTO, CheckoutEventDTO, EventSeatDTO, PaymentQuoteDTO, ProtectionQuoteDTO
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
        self.bookings = FakeBookingRepository()
        self._lock = asyncio.Lock()
        self.transaction_count = 0

    @asynccontextmanager
    async def transaction(self):
        async with self._lock:
            self.transaction_count += 1
            yield self


class FakeEventRepository:
    def __init__(self, exists: bool) -> None:
        self._exists = exists

    async def get_for_checkout(self, event_id: int) -> CheckoutEventDTO | None:
        if not self._exists:
            return None
        return CheckoutEventDTO(
            id=event_id,
            title="Test event",
            category="conference",
            starts_at=datetime(2030, 1, 1),
        )


class FakeEventSeatRepository:
    def __init__(self, seat_ids: set[int]) -> None:
        self._seat_ids = seat_ids
        self._available_seat_ids = set(seat_ids)

    async def get_existing_seat_ids(self, event_id: int, seat_ids: list[int]) -> set[int]:
        return self._seat_ids

    async def get_available_for_update(self, event_id: int, seat_ids: list[int]) -> list[EventSeatDTO]:
        return [
            EventSeatDTO(id=seat_id, event_id=event_id, seat_id=seat_id, price=5000)
            for seat_id in seat_ids
            if seat_id in self._available_seat_ids
        ]

    async def reserve(self, event_seat_ids: list[int], booking_id: int, reserved_until: datetime) -> None:
        self._available_seat_ids.difference_update(event_seat_ids)


class FakeBookingRepository:
    def __init__(self) -> None:
        self.created: list[BookingDTO] = []
        self.quotes: list[tuple[int, int, int | None]] = []

    async def create(self, event_id: int, user_id: int, amount: int, reserved_until: datetime) -> BookingDTO:
        booking = BookingDTO(
            id=len(self.created) + 1,
            event_id=event_id,
            user_id=user_id,
            amount=amount,
            reserved_until=reserved_until,
        )
        self.created.append(booking)
        return booking

    async def save_checkout_quote(
        self,
        booking_id: int,
        payment_commission: int,
        protection_price: int | None,
    ) -> None:
        self.quotes.append((booking_id, payment_commission, protection_price))


class FakePaymentClient:
    async def calculate(self, booking_id: int, amount: int) -> PaymentQuoteDTO:
        return PaymentQuoteDTO(
            commission=150,
            total=amount + 150,
            payment_methods=["bank_card"],
        )


class FakeProtectionClient:
    async def calculate(
        self,
        booking_id: int,
        ticket_amount: int,
        event_category: str,
        event_starts_at: str,
    ) -> ProtectionQuoteDTO:
        return ProtectionQuoteDTO(
            available=True,
            price=350,
            covered_amount=ticket_amount,
        )


@pytest.fixture
def payment_client() -> FakePaymentClient:
    return FakePaymentClient()


@pytest.fixture
def protection_client() -> FakeProtectionClient:
    return FakeProtectionClient()


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
