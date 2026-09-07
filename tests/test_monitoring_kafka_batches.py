import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest

from app.domain.dto import TicketPurchasedEvent
from monitoring.infrastructure.kafka import collect_purchase_batch, deserialize_ticket_purchase


class FakeBatchConsumer:
    def __init__(self, responses: list[dict[str, list[Any]]], *, wait_on_empty: bool = False) -> None:
        self.responses = responses
        self.wait_on_empty = wait_on_empty
        self.calls: list[dict[str, int]] = []

    async def getmany(self, *, timeout_ms: int, max_records: int) -> dict[str, list[Any]]:
        self.calls.append({"timeout_ms": timeout_ms, "max_records": max_records})
        response = self.responses.pop(0)
        if not response and self.wait_on_empty:
            await asyncio.sleep(timeout_ms / 1000)
        return response


def make_purchase_event(event_id: int) -> TicketPurchasedEvent:
    return TicketPurchasedEvent(
        payment_id=uuid4(),
        event_id=event_id,
        tickets_count=1,
        total_amount=1000,
        paid_at=datetime.now(UTC),
    )


def make_records(events: list[TicketPurchasedEvent]) -> dict[str, list[Any]]:
    return {"partition-0": [SimpleNamespace(value=event) for event in events]}


def test_deserialize_ticket_purchase() -> None:
    event = make_purchase_event(3)

    result = deserialize_ticket_purchase(event.model_dump_json().encode())

    assert result == event


@pytest.mark.asyncio
async def test_collect_purchase_batch_returns_immediately_after_ten_records() -> None:
    events = [make_purchase_event(index) for index in range(1, 11)]
    consumer = FakeBatchConsumer([make_records(events)])

    batch = await collect_purchase_batch(consumer, max_records=10, batch_timeout_ms=500)

    assert batch == events
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
    assert batch == events
    assert consumer.calls[0]["max_records"] == 10
    assert consumer.calls[1]["max_records"] == 4
    assert 0.02 <= elapsed < 0.2
