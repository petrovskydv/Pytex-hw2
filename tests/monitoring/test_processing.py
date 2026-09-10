from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from monitoring.domain.dto import PaymentActivityAggregate
from monitoring.infrastructure.database.models import EventPaymentActivity
from monitoring.infrastructure.database.repositories.payment_activity import PaymentActivityRepository
from monitoring.services.purchase_batches import PurchaseBatchProcessor, aggregate_purchase_batch
from tests.monitoring.factories import make_aggregate, make_purchase_event


def test_aggregate_purchase_batch_groups_by_event_id() -> None:
    """Проверяет группировку покупок по event_id и суммирование платежей, билетов и суммы."""
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
    """Проверяет, что пустой Kafka batch не создаёт агрегатов."""
    assert aggregate_purchase_batch([]) == []


@pytest.mark.asyncio
async def test_purchase_batch_processor_saves_and_returns_aggregates() -> None:
    """Проверяет, что processor агрегирует batch, сохраняет результат и возвращает те же агрегаты."""
    batch = [
        make_purchase_event(2, tickets_count=1, total_amount=1500),
        make_purchase_event(2, tickets_count=2, total_amount=3000),
    ]
    repository = AsyncMock(spec=PaymentActivityRepository)
    repository.save_batch.return_value = uuid4()
    processor = PurchaseBatchProcessor(repository)

    aggregates = await processor.process(batch)

    expected = [PaymentActivityAggregate(event_id=2, payments_count=2, tickets_count=3, total_amount=4500)]
    assert aggregates == expected
    repository.save_batch.assert_awaited_once_with(expected)


@pytest.mark.asyncio
async def test_save_batch_persists_all_aggregates_in_one_batch(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Проверяет сохранение всех агрегатов Kafka batch одной группой с общими batch_id и created_at."""
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
    """Проверяет, что каждый новый обработанный Kafka batch получает новый UUID batch_id."""
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
    """Проверяет атомарность: ошибка одной строки откатывает все агрегаты текущего batch."""
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
