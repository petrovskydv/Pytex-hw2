import asyncio
from datetime import UTC

import pytest

from app.domain.dto import TicketPurchasedEvent
from app.services.purchase_generator import PurchaseEventGenerator


class EventCollector:
    def __init__(self, expected_events: int) -> None:
        self.expected_events = expected_events
        self.events: list[TicketPurchasedEvent] = []
        self.ready = asyncio.Event()

    async def __call__(self, event: TicketPurchasedEvent) -> None:
        self.events.append(event)
        if len(self.events) >= self.expected_events:
            self.ready.set()


async def discard_event(_: TicketPurchasedEvent) -> None:
    return None


def test_purchase_event_contains_required_fields_and_repeated_event_ids() -> None:
    generator = PurchaseEventGenerator(discard_event, event_id_max=5)
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
    generator = PurchaseEventGenerator(
        collector,
        interval_seconds=0.001,
        event_id_max=5,
    )

    generator.start()
    await asyncio.wait_for(collector.ready.wait(), timeout=1)
    await generator.stop()

    generated_count = len(collector.events)
    await asyncio.sleep(0.01)

    assert generated_count >= 3
    assert len(collector.events) == generated_count

    await generator.stop()
