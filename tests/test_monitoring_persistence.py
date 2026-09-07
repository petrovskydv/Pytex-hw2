from uuid import UUID

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from monitoring.domain.dto import PaymentActivityAggregate
from monitoring.infrastructure.models import EventPaymentActivity
from monitoring.infrastructure.repositories import PaymentActivityRepository


def make_aggregate(
    event_id: int,
    *,
    payments_count: int = 1,
    tickets_count: int = 1,
    total_amount: int = 1000,
) -> PaymentActivityAggregate:
    return PaymentActivityAggregate(
        event_id=event_id,
        payments_count=payments_count,
        tickets_count=tickets_count,
        total_amount=total_amount,
    )


@pytest.mark.asyncio
async def test_save_batch_persists_all_aggregates_in_one_batch(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    repository = PaymentActivityRepository(session_factory)
    aggregates = [
        make_aggregate(3, payments_count=3, tickets_count=6, total_amount=12000),
        make_aggregate(1, payments_count=1, tickets_count=1, total_amount=3000),
        make_aggregate(5, payments_count=1, tickets_count=2, total_amount=5000),
    ]

    batch_id = await repository.save_batch(aggregates)

    async with session_factory() as session:
        rows = list((await session.scalars(select(EventPaymentActivity).order_by(EventPaymentActivity.id))).all())

    assert len(rows) == 3
    assert {row.batch_id for row in rows} == {batch_id}
    assert len({row.created_at for row in rows}) == 1
    assert rows[0].created_at.tzinfo is not None
    assert [(row.event_id, row.payments_count, row.tickets_count, row.total_amount) for row in rows] == [
        (3, 3, 6, 12000),
        (1, 1, 1, 3000),
        (5, 1, 2, 5000),
    ]


@pytest.mark.asyncio
async def test_save_batch_uses_new_batch_id_for_next_batch(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    repository = PaymentActivityRepository(session_factory)

    first_batch_id = await repository.save_batch([make_aggregate(1)])
    second_batch_id = await repository.save_batch([make_aggregate(2)])

    assert isinstance(first_batch_id, UUID)
    assert isinstance(second_batch_id, UUID)
    assert first_batch_id != second_batch_id


@pytest.mark.asyncio
async def test_save_batch_rolls_back_all_rows_on_error(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    repository = PaymentActivityRepository(session_factory)
    invalid_aggregate = PaymentActivityAggregate.model_construct(
        event_id=None,
        payments_count=1,
        tickets_count=1,
        total_amount=1000,
    )

    with pytest.raises(IntegrityError):
        await repository.save_batch([make_aggregate(1), invalid_aggregate])

    async with session_factory() as session:
        rows_count = await session.scalar(select(func.count()).select_from(EventPaymentActivity))

    assert rows_count == 0
