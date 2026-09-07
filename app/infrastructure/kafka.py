from collections.abc import Callable
from typing import Any

from aiokafka import AIOKafkaProducer

from app.config import KafkaSettings
from app.domain.dto import TicketPurchasedEvent

ProducerFactory = Callable[..., Any]


class KafkaPurchasePublisher:
    """Публикует события о состоявшихся покупках в Kafka."""

    def __init__(
        self,
        settings: KafkaSettings,
        producer_factory: ProducerFactory = AIOKafkaProducer,
    ) -> None:
        self._settings = settings
        self._producer_factory = producer_factory
        self._producer: Any | None = None

    async def start(self) -> None:
        """Подключает producer к Kafka."""
        if self._producer is not None:
            return

        producer = self._producer_factory(
            bootstrap_servers=self._settings.bootstrap_servers,
            linger_ms=self._settings.linger_ms,
        )
        self._producer = producer
        try:
            await producer.start()
        except BaseException:
            self._producer = None
            await producer.stop()
            raise

    async def stop(self) -> None:
        """Сбрасывает накопленные сообщения и закрывает producer."""
        producer = self._producer
        self._producer = None
        if producer is not None:
            await producer.stop()

    async def publish(self, event: TicketPurchasedEvent) -> None:
        """Ставит факт покупки в буфер Kafka producer."""
        producer = self._producer
        if producer is None:
            raise RuntimeError("Kafka producer не запущен")

        await producer.send(
            self._settings.topic,
            event.model_dump_json().encode("utf-8"),
        )
