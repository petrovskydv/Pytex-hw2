from pydantic import BaseModel


class PaymentActivityAggregate(BaseModel):
    """Агрегированная активность покупок по одному мероприятию в батче."""

    event_id: int
    payments_count: int
    tickets_count: int
    total_amount: int
