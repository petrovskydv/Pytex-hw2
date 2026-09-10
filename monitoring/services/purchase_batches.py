import logging
from collections.abc import Awaitable, Callable
from uuid import UUID

from monitoring.domain.dto import PaymentActivityAggregate, TicketPurchasedEvent

logger = logging.getLogger(__name__)

SavePaymentActivityBatch = Callable[[list[PaymentActivityAggregate]], Awaitable[UUID]]


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


async def process_purchase_batch(
    batch: list[TicketPurchasedEvent],
    *,
    save_aggregates: SavePaymentActivityBatch,
) -> list[PaymentActivityAggregate]:
    """Агрегирует Kafka-батч и сохраняет результат в PostgreSQL."""
    aggregates = aggregate_purchase_batch(batch)
    batch_id = await save_aggregates(aggregates)
    logger.info(
        "Обработан батч покупок %s: %s сообщений, %s мероприятий",
        batch_id,
        len(batch),
        len(aggregates),
    )
    return aggregates
