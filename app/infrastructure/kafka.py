from faststream.kafka import KafkaBroker

from app.domain.dto import TicketPurchasedEvent


class KafkaPurchasePublisher:
    """Публикует события о состоявшихся покупках в Kafka."""

    def __init__(self, broker: KafkaBroker, topic: str) -> None:
        self._broker = broker
        self._topic = topic

    async def publish(self, event: TicketPurchasedEvent) -> None:
        """Публикует факт покупки в Kafka."""
        await self._broker.publish(
            event,
            topic=self._topic,
            key=str(event.event_id).encode(),
            no_confirm=True,
        )
