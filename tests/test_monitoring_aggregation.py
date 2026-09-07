from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from app.domain.dto import TicketPurchasedEvent
from monitoring.domain.dto import PaymentActivityAggregate
from monitoring.services.purchase_batches import aggregate_purchase_batch, process_purchase_batch


def make_purchase_event(
    event_id: int,
    *,
    tickets_count: int,
    total_amount: int,
) -> TicketPurchasedEvent:
    return TicketPurchasedEvent(
        payment_id=uuid4(),
        event_id=event_id,
        tickets_count=tickets_count,
        total_amount=total_amount,
        paid_at=datetime.now(UTC),
    )


def test_aggregate_purchase_batch_groups_by_event_id() -> None:
    batch = [
        make_purchase_event(3, tickets_count=2, total_amount=4000),
        make_purchase_event(1, tickets_count=1, total_amount=3000),
        make_purchase_event(3, tickets_count=3, total_amount=6000),
        make_purchase_event(5, tickets_count=2, total_amount=5000),
        make_purchase_event(3, tickets_count=1, total_amount=2000),
    ]

    aggregates = aggregate_purchase_batch(batch)

    assert aggregates == [
        PaymentActivityAggregate(event_id=3, payments_count=3, tickets_count=6, total_amount=12000),
        PaymentActivityAggregate(event_id=1, payments_count=1, tickets_count=1, total_amount=3000),
        PaymentActivityAggregate(event_id=5, payments_count=1, tickets_count=2, total_amount=5000),
    ]


def test_aggregate_purchase_batch_returns_empty_list_for_empty_batch() -> None:
    assert aggregate_purchase_batch([]) == []


@pytest.mark.asyncio
async def test_process_purchase_batch_saves_and_returns_aggregates() -> None:
    batch = [
        make_purchase_event(2, tickets_count=1, total_amount=1500),
        make_purchase_event(2, tickets_count=2, total_amount=3000),
    ]
    save_aggregates = AsyncMock(return_value=uuid4())

    aggregates = await process_purchase_batch(batch, save_aggregates=save_aggregates)

    expected = [PaymentActivityAggregate(event_id=2, payments_count=2, tickets_count=3, total_amount=4500)]
    assert aggregates == expected
    save_aggregates.assert_awaited_once_with(expected)
