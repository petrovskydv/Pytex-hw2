from datetime import UTC, datetime
from uuid import uuid4

from monitoring.domain.dto import PaymentActivityAggregate, TicketPurchasedEvent


def make_purchase_event(
    event_id: int,
    *,
    tickets_count: int = 1,
    total_amount: int = 1000,
) -> TicketPurchasedEvent:
    return TicketPurchasedEvent(
        payment_id=uuid4(),
        event_id=event_id,
        tickets_count=tickets_count,
        total_amount=total_amount,
        paid_at=datetime.now(UTC),
    )


def make_aggregate(
    event_id: int = 3,
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
