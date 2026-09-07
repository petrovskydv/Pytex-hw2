import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import pytest

from app.config import KafkaSettings
from app.domain.dto import TicketPurchasedEvent
from app.infrastructure.kafka import KafkaPurchasePublisher


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


def build_event() -> TicketPurchasedEvent:
    return TicketPurchasedEvent(
        payment_id=UUID("12345678-1234-5678-1234-567812345678"),
        event_id=3,
        tickets_count=2,
        total_amount=4000,
        paid_at=datetime(2026, 9, 7, 12, 0, tzinfo=UTC),
    )


@pytest.mark.asyncio
async def test_publisher_configures_linger_and_publishes_event() -> None:
    captured: dict[str, Any] = {}
    settings = KafkaSettings(bootstrap_servers="kafka:19092", topic="tickets.purchased", linger_ms=75)
    publisher = KafkaPurchasePublisher(settings, producer_factory=FakeProducerFactory(captured))

    await publisher.start()
    await publisher.publish(build_event())
    await publisher.stop()

    assert captured["kwargs"] == {
        "bootstrap_servers": "kafka:19092",
        "linger_ms": 75,
    }
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
