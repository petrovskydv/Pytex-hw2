import logging

from app.domain.dto import TicketPurchasedEvent
from monitoring.domain.dto import PaymentActivityAggregate

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


async def process_purchase_batch(batch: list[TicketPurchasedEvent]) -> list[PaymentActivityAggregate]:
    """Агрегирует полученный из Kafka батч покупок."""
    aggregates = aggregate_purchase_batch(batch)
    logger.info(
        "Агрегирован батч покупок: %s сообщений, %s мероприятий",
        len(batch),
        len(aggregates),
    )
    return aggregates
