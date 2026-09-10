import asyncio
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import pytest

from app.domain.dto import TicketPurchasedEvent
from app.infrastructure.kafka import KafkaPurchasePublisher
from app.services.purchase_generator import PurchaseEventGenerator


class EventCollector:
    def __init__(self, expected_events: int) -> None:
        self.expected_events = expected_events
        self.events: list[TicketPurchasedEvent] = []
        self.ready = asyncio.Event()

    async def publish(self, event: TicketPurchasedEvent) -> None:
        self.events.append(event)
        if len(self.events) >= self.expected_events:
            self.ready.set()


class NullPublisher:
    async def publish(self, _: TicketPurchasedEvent) -> None:
        return None


class FakeBroker:
    def __init__(self, captured: dict[str, Any]) -> None:
        self.captured = captured

    async def publish(
        self,
        message: TicketPurchasedEvent,
        *,
        topic: str,
        key: bytes,
        no_confirm: bool,
    ) -> object:
        self.captured.setdefault("messages", []).append((topic, message, key, no_confirm))
        return object()


def build_event() -> TicketPurchasedEvent:
    return TicketPurchasedEvent(
        payment_id=UUID("12345678-1234-5678-1234-567812345678"),
        event_id=3,
        tickets_count=2,
        total_amount=4000,
        paid_at=datetime(2026, 9, 7, 12, 0, tzinfo=UTC),
    )


def test_purchase_event_contains_required_fields_and_repeated_event_ids() -> None:
    generator = PurchaseEventGenerator(NullPublisher(), event_id_max=5)
    events = [generator.create_event() for _ in range(6)]

    assert len({event.payment_id for event in events}) == 6
    assert len({event.event_id for event in events}) < 6

    for event in events:
        assert 1 <= event.event_id <= 5
        assert 1 <= event.tickets_count <= 4
        assert event.total_amount > 0
        assert event.total_amount % event.tickets_count == 0
        assert event.paid_at.tzinfo is UTC
        assert set(event.model_dump()) == {
            "payment_id",
            "event_id",
            "tickets_count",
            "total_amount",
            "paid_at",
        }


@pytest.mark.asyncio
async def test_purchase_generator_runs_in_background_and_stops() -> None:
    collector = EventCollector(expected_events=3)
    generator = PurchaseEventGenerator(collector, interval_seconds=0.001, event_id_max=5)

    generator.start()
    await asyncio.wait_for(collector.ready.wait(), timeout=1)
    await generator.stop()

    generated_count = len(collector.events)
    await asyncio.sleep(0.01)

    assert generated_count >= 3
    assert len(collector.events) == generated_count

    await generator.stop()


@pytest.mark.asyncio
async def test_publisher_uses_injected_broker_and_event_id_key() -> None:
    captured: dict[str, Any] = {}
    broker = FakeBroker(captured)
    publisher = KafkaPurchasePublisher(broker, "tickets.purchased")
    event = build_event()

    await publisher.publish(event)

    assert captured["messages"] == [("tickets.purchased", event, b"3", True)]
