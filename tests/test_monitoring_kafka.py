import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest

import monitoring.main as monitoring_main
from app.domain.dto import TicketPurchasedEvent
from monitoring.config import KafkaSettings, WebSocketSettings
from monitoring.domain.dto import PaymentActivityAggregate
from monitoring.infrastructure.kafka import (
    MonitoringKafka,
    collect_purchase_batch,
    deserialize_ticket_purchase,
)
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


class FakeBatchConsumer:
    def __init__(
        self,
        responses: list[dict[str, list[Any]]] | None = None,
        *,
        wait_on_empty: bool = False,
        order: list[str] | None = None,
    ) -> None:
        self.responses = responses or []
        self.wait_on_empty = wait_on_empty
        self.order = order
        self.calls: list[dict[str, int]] = []
        self.commit_calls: list[dict[str, int]] = []
        self.seek_calls: list[tuple[str, int]] = []
        self.last_response: dict[str, list[Any]] | None = None
        self.redeliver = False
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def getmany(self, *, timeout_ms: int, max_records: int) -> dict[str, list[Any]]:
        self.calls.append({"timeout_ms": timeout_ms, "max_records": max_records})
        if self.redeliver and self.last_response is not None:
            self.redeliver = False
            return self.last_response
        if self.responses:
            response = self.responses.pop(0)
            if response:
                self.last_response = response
            if not response and self.wait_on_empty:
                await asyncio.sleep(timeout_ms / 1000)
            return response
        await asyncio.Event().wait()
        return {}

    async def commit(self, offsets: dict[str, int]) -> None:
        self.commit_calls.append(offsets)
        if self.order is not None:
            self.order.append("kafka.commit")

    def seek(self, partition: str, offset: int) -> None:
        self.seek_calls.append((partition, offset))
        self.redeliver = True
        if self.order is not None:
            self.order.append("seek")


class ConsumerFactory:
    def __init__(self, consumer: FakeBatchConsumer) -> None:
        self.consumer = consumer
        self.args: tuple[Any, ...] = ()
        self.kwargs: dict[str, Any] = {}

    def __call__(self, *args: Any, **kwargs: Any) -> FakeBatchConsumer:
        self.args = args
        self.kwargs = kwargs
        return self.consumer


class RecordingBatchHandler:
    def __init__(self, order: list[str], aggregates: list[PaymentActivityAggregate]) -> None:
        self.order = order
        self.aggregates = aggregates

    async def __call__(self, batch: list[TicketPurchasedEvent]) -> list[PaymentActivityAggregate]:
        self.order.append("db.commit")
        return self.aggregates


class FailOnceBatchHandler:
    def __init__(self, order: list[str], aggregates: list[PaymentActivityAggregate]) -> None:
        self.order = order
        self.aggregates = aggregates
        self.calls = 0

    async def __call__(self, batch: list[TicketPurchasedEvent]) -> list[PaymentActivityAggregate]:
        self.calls += 1
        if self.calls == 1:
            self.order.append("db.error")
            raise RuntimeError("database unavailable")
        self.order.append("db.commit")
        return self.aggregates


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


def make_records(
    events: list[TicketPurchasedEvent],
    *,
    start_offset: int = 0,
    partition: str = "partition-0",
) -> dict[str, list[Any]]:
    return {
        partition: [SimpleNamespace(value=event, offset=start_offset + index) for index, event in enumerate(events)]
    }


async def ignore_purchase_batch(batch: list[TicketPurchasedEvent]) -> list[PaymentActivityAggregate]:
    return []


@pytest.mark.asyncio
async def test_monitoring_lifespan_owns_resources(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    kafka = FakeKafka(calls)
    monkeypatch.setattr(monitoring_main, "get_settings", make_settings)
    monkeypatch.setattr(monitoring_main, "MonitoringKafka", lambda _settings, _handler, _queue: kafka)
    monkeypatch.setattr(monitoring_main, "engine", FakeEngine(calls))

    app = monitoring_main.app
    async with app.router.lifespan_context(app):
        assert app.state.kafka is kafka
        assert isinstance(app.state.payment_activity_queue, asyncio.Queue)
        assert isinstance(app.state.websocket_manager, WebSocketConnectionManager)
        assert app.state.websocket_worker is not None
        assert calls == ["kafka.start"]

    assert calls == ["kafka.start", "kafka.stop", "database.dispose"]


@pytest.mark.asyncio
async def test_monitoring_lifespan_cleans_up_after_kafka_start_error(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    kafka = FakeKafka(calls, fail_start=True)
    monkeypatch.setattr(monitoring_main, "get_settings", make_settings)
    monkeypatch.setattr(monitoring_main, "MonitoringKafka", lambda _settings, _handler, _queue: kafka)
    monkeypatch.setattr(monitoring_main, "engine", FakeEngine(calls))

    app = monitoring_main.app
    with pytest.raises(RuntimeError, match="Kafka unavailable"):
        async with app.router.lifespan_context(app):
            pass

    assert calls == ["kafka.start", "kafka.stop", "database.dispose"]


@pytest.mark.asyncio
async def test_kafka_connection_disables_auto_commit() -> None:
    consumer = FakeBatchConsumer()
    factory = ConsumerFactory(consumer)
    connection = MonitoringKafka(
        KafkaSettings(),
        batch_handler=ignore_purchase_batch,
        payment_activity_queue=asyncio.Queue(),
        consumer_factory=factory,
    )

    await connection.start()
    await asyncio.sleep(0)
    await connection.stop()

    assert factory.args == ("tickets.purchased",)
    assert factory.kwargs["bootstrap_servers"] == "localhost:9092"
    assert factory.kwargs["group_id"] == "payment-monitor"
    assert factory.kwargs["enable_auto_commit"] is False
    assert factory.kwargs["value_deserializer"] is deserialize_ticket_purchase
    assert consumer.calls[0]["max_records"] == 10
    assert 0 < consumer.calls[0]["timeout_ms"] <= 500
    assert consumer.started is True
    assert consumer.stopped is True


def test_deserialize_ticket_purchase() -> None:
    event = make_purchase_event(3)
    assert deserialize_ticket_purchase(event.model_dump_json().encode()) == event


@pytest.mark.asyncio
async def test_collect_purchase_batch_returns_immediately_after_ten_records() -> None:
    events = [make_purchase_event(index) for index in range(1, 11)]
    consumer = FakeBatchConsumer([make_records(events)])

    batch = await collect_purchase_batch(consumer, max_records=10, batch_timeout_ms=500)

    assert batch.events == events
    assert batch.retry_offsets == {"partition-0": 0}
    assert batch.commit_offsets == {"partition-0": 10}
    assert len(consumer.calls) == 1
    assert consumer.calls[0]["max_records"] == 10
    assert 0 < consumer.calls[0]["timeout_ms"] <= 500


@pytest.mark.asyncio
async def test_collect_purchase_batch_flushes_partial_batch_on_timeout() -> None:
    events = [make_purchase_event(index) for index in range(1, 7)]
    consumer = FakeBatchConsumer([make_records(events), {}], wait_on_empty=True)
    loop = asyncio.get_running_loop()
    started_at = loop.time()

    batch = await collect_purchase_batch(consumer, max_records=10, batch_timeout_ms=30)

    elapsed = loop.time() - started_at
    assert batch.events == events
    assert batch.retry_offsets == {"partition-0": 0}
    assert batch.commit_offsets == {"partition-0": 6}
    assert consumer.calls[0]["max_records"] == 10
    assert consumer.calls[1]["max_records"] == 4
    assert 0.02 <= elapsed < 0.2


@pytest.mark.asyncio
async def test_monitoring_enqueues_aggregates_after_kafka_commit() -> None:
    order: list[str] = []
    events = [make_purchase_event(1), make_purchase_event(2)]
    aggregates = [make_aggregate(1), make_aggregate(2)]
    consumer = FakeBatchConsumer([make_records(events, start_offset=4)], order=order)
    queue = RecordingPaymentActivityQueue(order)
    connection = MonitoringKafka(
        KafkaSettings(max_records=2, batch_timeout_ms=30),
        batch_handler=RecordingBatchHandler(order, aggregates),
        payment_activity_queue=queue,
        consumer_factory=ConsumerFactory(consumer),
    )

    await connection.start()
    try:
        queued_aggregates = await asyncio.wait_for(queue.get(), timeout=1)
    finally:
        await connection.stop()

    assert queued_aggregates == aggregates
    assert order[:3] == ["db.commit", "kafka.commit", "queue.put"]
    assert consumer.commit_calls == [{"partition-0": 6}]


@pytest.mark.asyncio
async def test_monitoring_enqueues_only_after_retry_succeeds_and_commits() -> None:
    order: list[str] = []
    events = [make_purchase_event(1), make_purchase_event(1)]
    aggregates = [make_aggregate(1)]
    consumer = FakeBatchConsumer([make_records(events, start_offset=8)], order=order)
    handler = FailOnceBatchHandler(order, aggregates)
    queue = RecordingPaymentActivityQueue(order)
    connection = MonitoringKafka(
        KafkaSettings(max_records=2, batch_timeout_ms=30),
        batch_handler=handler,
        payment_activity_queue=queue,
        consumer_factory=ConsumerFactory(consumer),
    )

    await connection.start()
    try:
        queued_aggregates = await asyncio.wait_for(queue.get(), timeout=1)
    finally:
        await connection.stop()

    assert queued_aggregates == aggregates
    assert handler.calls == 2
    assert order[:5] == ["db.error", "seek", "db.commit", "kafka.commit", "queue.put"]
    assert consumer.seek_calls == [("partition-0", 8)]
    assert consumer.commit_calls == [{"partition-0": 10}]
