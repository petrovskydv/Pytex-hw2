from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class TicketPurchasedEvent(BaseModel):
    """Факт состоявшейся покупки билетов, полученный из Kafka."""

    payment_id: UUID
    event_id: int
    tickets_count: int
    total_amount: int
    paid_at: datetime


class PaymentActivityAggregate(BaseModel):
    """Агрегированная активность покупок по одному мероприятию в батче."""

    event_id: int
    payments_count: int
    tickets_count: int
    total_amount: int
