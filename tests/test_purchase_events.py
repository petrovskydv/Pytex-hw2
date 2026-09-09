import asyncio
import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import pytest

from app.config import KafkaSettings
from app.domain.dto import TicketPurchasedEvent
from app.infrastructure.kafka import KafkaPurchasePublisher
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


class FakeProducer:
    def __init__(self, captured: dict[str, Any]) -> None:
        self.captured = captured

    async def start(self) -> None:
        self.captured["started"] = True

    async def stop(self) -> None:
        self.captured["stopped"] = True

    async def send(self, topic: str, value: bytes) -> object:
        self.captured.setdefault("messages", []).append((topic, value))
        return object()


class FailingProducer(FakeProducer):
    async def start(self) -> None:
        self.captured["started"] = True
        raise RuntimeError("Kafka unavailable")


class FakeProducerFactory:
    def __init__(self, captured: dict[str, Any], producer_type: type[FakeProducer] = FakeProducer) -> None:
        self.captured = captured
        self.producer_type = producer_type

    def __call__(self, **kwargs: Any) -> FakeProducer:
        self.captured["kwargs"] = kwargs
        return self.producer_type(self.captured)


async def discard_event(_: TicketPurchasedEvent) -> None:
    return None


def build_event() -> TicketPurchasedEvent:
    return TicketPurchasedEvent(
        payment_id=UUID("12345678-1234-5678-1234-567812345678"),
        event_id=3,
        tickets_count=2,
        total_amount=4000,
        paid_at=datetime(2026, 9, 7, 12, 0, tzinfo=UTC),
    )


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
async def test_publisher_configures_linger_and_publishes_event() -> None:
    captured: dict[str, Any] = {}
    settings = KafkaSettings(bootstrap_servers="kafka:19092", topic="tickets.purchased", linger_ms=75)
    publisher = KafkaPurchasePublisher(settings, producer_factory=FakeProducerFactory(captured))

    await publisher.start()
    await publisher.publish(build_event())
    await publisher.stop()

    assert captured["kwargs"] == {"bootstrap_servers": "kafka:19092", "linger_ms": 75}
    assert captured["started"] is True
    assert captured["stopped"] is True

    [(topic, raw_value)] = captured["messages"]
    assert topic == "tickets.purchased"
    assert json.loads(raw_value) == {
        "payment_id": "12345678-1234-5678-1234-567812345678",
        "event_id": 3,
        "tickets_count": 2,
        "total_amount": 4000,
        "paid_at": "2026-09-07T12:00:00Z",
    }


@pytest.mark.asyncio
async def test_publisher_cleans_up_after_start_error() -> None:
    captured: dict[str, Any] = {}
    publisher = KafkaPurchasePublisher(
        KafkaSettings(),
        producer_factory=FakeProducerFactory(captured, FailingProducer),
    )

    with pytest.raises(RuntimeError, match="Kafka unavailable"):
        await publisher.start()

    assert captured["started"] is True
    assert captured["stopped"] is True
    await publisher.stop()


@pytest.mark.asyncio
async def test_publisher_rejects_publish_before_start() -> None:
    publisher = KafkaPurchasePublisher(KafkaSettings(), producer_factory=FakeProducerFactory({}))

    with pytest.raises(RuntimeError, match="producer не запущен"):
        await publisher.publish(build_event())
