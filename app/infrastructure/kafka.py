from collections.abc import Callable
from typing import Any

from faststream.kafka import KafkaBroker

from app.config import KafkaSettings
from app.domain.dto import TicketPurchasedEvent

BrokerFactory = Callable[..., Any]


class KafkaPurchasePublisher:
    """Публикует события о состоявшихся покупках в Kafka."""

    def __init__(
        self,
        settings: KafkaSettings,
        broker_factory: BrokerFactory = KafkaBroker,
    ) -> None:
        self._settings = settings
        self._broker = broker_factory(
            settings.bootstrap_servers,
            linger_ms=settings.linger_ms,
        )
        self._started = False

    async def start(self) -> None:
        """Подключает producer к Kafka."""
        if self._started:
            return

        try:
            await self._broker.start()
        except BaseException:
            await self._broker.stop()
            raise
        self._started = True

    async def stop(self) -> None:
        """Сбрасывает накопленные сообщения и закрывает producer."""
        if not self._started:
            return

        self._started = False
        await self._broker.stop()

    async def publish(self, event: TicketPurchasedEvent) -> None:
        """Ставит факт покупки в буфер Kafka producer."""
        if not self._started:
            raise RuntimeError("Kafka producer не запущен")

        await self._broker.publish(
            event,
            topic=self._settings.topic,
            key=str(event.event_id).encode(),
            no_confirm=True,
        )
