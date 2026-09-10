import logging

from monitoring.domain.dto import PaymentActivityAggregate, TicketPurchasedEvent
from monitoring.infrastructure.database.repositories.payment_activity import PaymentActivityRepository

logger = logging.getLogger(__name__)


def aggregate_purchase_batch(batch: list[TicketPurchasedEvent]) -> list[PaymentActivityAggregate]:
    """Агрегирует покупки одного батча по идентификатору мероприятия."""
    counters: dict[int, dict[str, int]] = {}

    for purchase in batch:
        aggregate = counters.setdefault(
            purchase.event_id,
            {"payments_count": 0, "tickets_count": 0, "total_amount": 0},
        )
        aggregate["payments_count"] += 1
        aggregate["tickets_count"] += purchase.tickets_count
        aggregate["total_amount"] += purchase.total_amount

    return [PaymentActivityAggregate(event_id=event_id, **aggregate) for event_id, aggregate in counters.items()]


class PurchaseBatchProcessor:
    """Агрегирует Kafka-батч и сохраняет результат в PostgreSQL."""

    def __init__(self, repository: PaymentActivityRepository) -> None:
        self._repository = repository

    async def process(self, batch: list[TicketPurchasedEvent]) -> list[PaymentActivityAggregate]:
        aggregates = aggregate_purchase_batch(batch)
        batch_id = await self._repository.save_batch(aggregates)
        logger.info(
            "Обработан батч покупок %s: %s сообщений, %s мероприятий",
            batch_id,
            len(batch),
            len(aggregates),
        )
        return aggregates
