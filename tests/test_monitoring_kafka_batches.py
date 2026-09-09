import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest

from app.domain.dto import TicketPurchasedEvent
from monitoring.config import KafkaSettings
from monitoring.infrastructure.kafka import (
    MonitoringKafka,
    collect_purchase_batch,
    deserialize_ticket_purchase,
)


class FakeBatchConsumer:
    def __init__(
        self,
        responses: list[dict[str, list[Any]]],
        *,
        wait_on_empty: bool = False,
        order: list[str] | None = None,
        commit_event: asyncio.Event | None = None,
    ) -> None:
        self.responses = responses
        self.wait_on_empty = wait_on_empty
        self.order = order
        self.commit_event = commit_event
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
        if self.commit_event is not None:
            self.commit_event.set()

    def seek(self, partition: str, offset: int) -> None:
        self.seek_calls.append((partition, offset))
        self.redeliver = True
        if self.order is not None:
            self.order.append("seek")


class StaticConsumerFactory:
    def __init__(self, consumer: FakeBatchConsumer) -> None:
        self.consumer = consumer

    def __call__(self, *args: Any, **kwargs: Any) -> FakeBatchConsumer:
        return self.consumer


class RecordingBatchHandler:
    def __init__(self, order: list[str]) -> None:
        self.order = order

    async def __call__(self, batch: list[TicketPurchasedEvent]) -> None:
        self.order.append("db.commit")


class FailOnceBatchHandler:
    def __init__(self, order: list[str]) -> None:
        self.order = order
        self.calls = 0

    async def __call__(self, batch: list[TicketPurchasedEvent]) -> None:
        self.calls += 1
        if self.calls == 1:
            self.order.append("db.error")
            raise RuntimeError("database unavailable")
        self.order.append("db.commit")


def make_purchase_event(event_id: int) -> TicketPurchasedEvent:
    return TicketPurchasedEvent(
        payment_id=uuid4(),
        event_id=event_id,
        tickets_count=1,
        total_amount=1000,
        paid_at=datetime.now(UTC),
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


def test_deserialize_ticket_purchase() -> None:
    event = make_purchase_event(3)

    result = deserialize_ticket_purchase(event.model_dump_json().encode())

    assert result == event


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
async def test_monitoring_commits_offsets_after_batch_handler() -> None:
    order: list[str] = []
    committed = asyncio.Event()
    events = [make_purchase_event(1), make_purchase_event(2)]
    consumer = FakeBatchConsumer(
        [make_records(events, start_offset=4)],
        order=order,
        commit_event=committed,
    )
    handler = RecordingBatchHandler(order)
    connection = MonitoringKafka(
        KafkaSettings(max_records=2, batch_timeout_ms=30),
        batch_handler=handler,
        consumer_factory=StaticConsumerFactory(consumer),
    )

    await connection.start()
    try:
        await asyncio.wait_for(committed.wait(), timeout=1)
    finally:
        await connection.stop()

    assert order[:2] == ["db.commit", "kafka.commit"]
    assert consumer.commit_calls == [{"partition-0": 6}]


@pytest.mark.asyncio
async def test_monitoring_does_not_commit_failed_batch_and_redelivers() -> None:
    order: list[str] = []
    committed = asyncio.Event()
    events = [make_purchase_event(1), make_purchase_event(1)]
    consumer = FakeBatchConsumer(
        [make_records(events, start_offset=8)],
        order=order,
        commit_event=committed,
    )
    handler = FailOnceBatchHandler(order)
    connection = MonitoringKafka(
        KafkaSettings(max_records=2, batch_timeout_ms=30),
        batch_handler=handler,
        consumer_factory=StaticConsumerFactory(consumer),
    )

    await connection.start()
    try:
        await asyncio.wait_for(committed.wait(), timeout=1)
    finally:
        await connection.stop()

    assert handler.calls == 2
    assert order[:4] == ["db.error", "seek", "db.commit", "kafka.commit"]
    assert consumer.seek_calls == [("partition-0", 8)]
    assert consumer.commit_calls == [{"partition-0": 10}]
