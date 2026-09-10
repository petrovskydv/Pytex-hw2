import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest
from faststream import AckPolicy

import monitoring.main as monitoring_main
from monitoring.config import KafkaSettings, WebSocketSettings
from monitoring.domain.dto import PaymentActivityAggregate, TicketPurchasedEvent
from monitoring.infrastructure.kafka import MonitoringKafka
from monitoring.services.websocket_delivery import WebSocketConnectionManager


class FakeKafka:
    def __init__(self, calls: list[str], *, fail_start: bool = False) -> None:
        self.calls = calls
        self.fail_start = fail_start

    async def start(self) -> None:
        self.calls.append("kafka.start")
        if self.fail_start:
            raise RuntimeError("Kafka unavailable")

    async def stop(self) -> None:
        self.calls.append("kafka.stop")


class FakeEngine:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls

    async def dispose(self) -> None:
        self.calls.append("database.dispose")


class FakeSubscriber:
    def __init__(self, captured: dict[str, Any]) -> None:
        self.captured = captured

    def __call__(self, handler: Any) -> Any:
        self.captured["handler"] = handler
        return handler


class FakeBroker:
    def __init__(self, captured: dict[str, Any]) -> None:
        self.captured = captured

    def subscriber(self, *args: Any, **kwargs: Any) -> FakeSubscriber:
        self.captured["subscriber_args"] = args
        self.captured["subscriber_kwargs"] = kwargs
        return FakeSubscriber(self.captured)


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


def make_settings() -> SimpleNamespace:
    return SimpleNamespace(kafka=KafkaSettings(), websocket=WebSocketSettings())


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
async def test_monitoring_lifespan_owns_kafka_broker(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    broker = FakeKafka(calls)
    consumer = object()
    captured: dict[str, Any] = {}

    def create_broker(*args: Any, **kwargs: Any) -> FakeKafka:
        captured["broker_args"] = args
        captured["broker_kwargs"] = kwargs
        return broker

    monkeypatch.setattr(monitoring_main, "get_settings", make_settings)
    monkeypatch.setattr(monitoring_main, "KafkaBroker", create_broker)
    monkeypatch.setattr(monitoring_main, "MonitoringKafka", lambda _broker, _settings, _processor, _queue: consumer)
    monkeypatch.setattr(monitoring_main, "engine", FakeEngine(calls))

    app = monitoring_main.app
    async with app.router.lifespan_context(app):
        assert app.state.kafka_broker is broker
        assert app.state.kafka is consumer
        assert isinstance(app.state.payment_activity_queue, asyncio.Queue)
        assert isinstance(app.state.websocket_manager, WebSocketConnectionManager)
        assert app.state.websocket_worker is not None
        assert calls == ["kafka.start"]

    assert captured["broker_args"] == ("localhost:9092",)
    assert captured["broker_kwargs"] == {"consumer_only": True}
    assert calls == ["kafka.start", "kafka.stop", "database.dispose"]


@pytest.mark.asyncio
async def test_monitoring_lifespan_cleans_up_after_kafka_start_error(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    broker = FakeKafka(calls, fail_start=True)

    monkeypatch.setattr(monitoring_main, "get_settings", make_settings)
    monkeypatch.setattr(monitoring_main, "KafkaBroker", lambda *_args, **_kwargs: broker)
    monkeypatch.setattr(monitoring_main, "MonitoringKafka", lambda _broker, _settings, _processor, _queue: object())
    monkeypatch.setattr(monitoring_main, "engine", FakeEngine(calls))

    app = monitoring_main.app
    with pytest.raises(RuntimeError, match="Kafka unavailable"):
        async with app.router.lifespan_context(app):
            pass

    assert calls == ["kafka.start", "kafka.stop", "database.dispose"]


def test_faststream_subscriber_uses_injected_broker_and_required_settings() -> None:
    captured: dict[str, Any] = {}
    broker = FakeBroker(captured)
    connection = MonitoringKafka(
        broker,
        KafkaSettings(),
        processor=RecordingBatchProcessor([], []),
        payment_activity_queue=asyncio.Queue(),
    )

    assert captured["subscriber_args"] == ("tickets.purchased",)
    assert captured["subscriber_kwargs"] == {
        "group_id": "payment-monitor",
        "batch": True,
        "max_records": 10,
        "batch_timeout_ms": 500,
        "auto_offset_reset": "earliest",
        "ack_policy": AckPolicy.MANUAL,
    }
    assert captured["handler"] == connection._handle_batch


@pytest.mark.asyncio
async def test_monitoring_acks_and_enqueues_only_after_database_commit() -> None:
    order: list[str] = []
    aggregates = [make_aggregate(1), make_aggregate(2)]
    queue = RecordingPaymentActivityQueue(order)
    captured: dict[str, Any] = {}
    broker = FakeBroker(captured)
    MonitoringKafka(
        broker,
        KafkaSettings(),
        processor=RecordingBatchProcessor(order, aggregates),
        payment_activity_queue=queue,
    )
    message = FakeMessage(order)

    await captured["handler"]([make_purchase_event(1), make_purchase_event(2)], message)

    assert order == ["db.commit", "kafka.ack", "queue.put"]
    assert message.ack_calls == 1
    assert message.nack_calls == 0
    assert queue.get_nowait() == aggregates


@pytest.mark.asyncio
async def test_monitoring_nacks_without_enqueuing_after_database_error() -> None:
    order: list[str] = []
    queue = RecordingPaymentActivityQueue(order)
    captured: dict[str, Any] = {}
    broker = FakeBroker(captured)
    MonitoringKafka(
        broker,
        KafkaSettings(),
        processor=FailingBatchProcessor(order),
        payment_activity_queue=queue,
    )
    message = FakeMessage(order)

    await captured["handler"]([make_purchase_event(1)], message)

    assert order == ["db.error", "kafka.nack"]
    assert message.ack_calls == 0
    assert message.nack_calls == 1
    assert queue.empty()
