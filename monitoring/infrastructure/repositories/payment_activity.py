from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from monitoring.domain.dto import PaymentActivityAggregate
from monitoring.infrastructure.models import EventPaymentActivity


class PaymentActivityRepository:
    """Сохраняет агрегированную активность оплат в PostgreSQL."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def save_batch(self, aggregates: list[PaymentActivityAggregate]) -> UUID:
        """Сохраняет все агрегаты батча одной транзакцией и возвращает batch_id."""
        batch_id = uuid4()
        created_at = datetime.now(UTC)
        rows = [
            EventPaymentActivity(
                batch_id=batch_id,
                event_id=aggregate.event_id,
                payments_count=aggregate.payments_count,
                tickets_count=aggregate.tickets_count,
                total_amount=aggregate.total_amount,
                created_at=created_at,
            )
            for aggregate in aggregates
        ]

        async with self._session_factory() as session, session.begin():
            session.add_all(rows)

        return batch_id
