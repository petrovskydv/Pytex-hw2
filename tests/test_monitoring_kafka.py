import asyncio
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from monitoring.config import KafkaSettings
from monitoring.domain.dto import PaymentActivityAggregate, TicketPurchasedEvent
from monitoring.infrastructure.kafka import MonitoringKafka


class FakeBroker:
    def __init__(self) -> None:
        self.handler: Any = None

    def subscriber(self, *args: Any, **kwargs: Any) -> Any:
        def register(handler: Any) -> Any:
            self.handler = handler
            return handler

        return register


class FakeMessage:
    def __init__(self, order: list[str]) -> None:
        self.order = order
        self.ack_calls = 0
        self.nack_calls = 0

    async def ack(self) -> None:
        self.ack_calls += 1
        self.order.append("kafka.ack")

    async def nack(self) -> None:
        self.nack_calls += 1
        self.order.append("kafka.nack")


class RecordingBatchProcessor:
    def __init__(self, order: list[str], aggregates: list[PaymentActivityAggregate]) -> None:
        self.order = order
        self.aggregates = aggregates

    async def process(self, batch: list[TicketPurchasedEvent]) -> list[PaymentActivityAggregate]:
        self.order.append("db.commit")
        return self.aggregates


class FailingBatchProcessor:
    def __init__(self, order: list[str]) -> None:
        self.order = order

    async def process(self, batch: list[TicketPurchasedEvent]) -> list[PaymentActivityAggregate]:
        self.order.append("db.error")
        raise RuntimeError("database unavailable")


class RecordingPaymentActivityQueue(asyncio.Queue[list[PaymentActivityAggregate]]):
    def __init__(self, order: list[str]) -> None:
        super().__init__()
        self.order = order

    async def put(self, item: list[PaymentActivityAggregate]) -> None:
        self.order.append("queue.put")
        await super().put(item)


def make_purchase_event(event_id: int) -> TicketPurchasedEvent:
    return TicketPurchasedEvent(
        payment_id=uuid4(),
        event_id=event_id,
        tickets_count=1,
        total_amount=1000,
        paid_at=datetime.now(UTC),
    )


def make_aggregate(event_id: int) -> PaymentActivityAggregate:
    return PaymentActivityAggregate(
        event_id=event_id,
        payments_count=1,
        tickets_count=1,
        total_amount=1000,
    )


@pytest.mark.asyncio
async def test_monitoring_acks_and_enqueues_only_after_database_commit() -> None:
    """Проверяет обязательный порядок: commit БД, затем Kafka ACK, затем asyncio.Queue."""
    order: list[str] = []
    aggregates = [make_aggregate(1), make_aggregate(2)]
    queue = RecordingPaymentActivityQueue(order)
    broker = FakeBroker()
    MonitoringKafka(
        broker,
        KafkaSettings(),
        processor=RecordingBatchProcessor(order, aggregates),
        payment_activity_queue=queue,
    )
    message = FakeMessage(order)

    await broker.handler([make_purchase_event(1), make_purchase_event(2)], message)

    assert order == ["db.commit", "kafka.ack", "queue.put"]
    assert message.ack_calls == 1
    assert message.nack_calls == 0
    assert queue.get_nowait() == aggregates


@pytest.mark.asyncio
async def test_monitoring_nacks_without_enqueuing_after_database_error() -> None:
    """Проверяет NACK и отсутствие данных в asyncio.Queue при ошибке сохранения batch в БД."""
    order: list[str] = []
    queue = RecordingPaymentActivityQueue(order)
    broker = FakeBroker()
    MonitoringKafka(
        broker,
        KafkaSettings(),
        processor=FailingBatchProcessor(order),
        payment_activity_queue=queue,
    )
    message = FakeMessage(order)

    await broker.handler([make_purchase_event(1)], message)

    assert order == ["db.error", "kafka.nack"]
    assert message.ack_calls == 0
    assert message.nack_calls == 1
    assert queue.empty()
