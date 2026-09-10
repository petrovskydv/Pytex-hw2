import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from faststream import AckPolicy
from faststream.kafka import KafkaBroker, KafkaMessage

from monitoring.config import KafkaSettings
from monitoring.domain.dto import PaymentActivityAggregate, TicketPurchasedEvent

logger = logging.getLogger(__name__)

BrokerFactory = Callable[..., Any]
PurchaseBatchHandler = Callable[[list[TicketPurchasedEvent]], Awaitable[list[PaymentActivityAggregate]]]


class MonitoringKafka:
    """Получает Kafka-события батчами через FastStream."""

    def __init__(
        self,
        settings: KafkaSettings,
        batch_handler: PurchaseBatchHandler,
        payment_activity_queue: asyncio.Queue[list[PaymentActivityAggregate]],
        broker_factory: BrokerFactory = KafkaBroker,
    ) -> None:
        self._batch_handler = batch_handler
        self._payment_activity_queue = payment_activity_queue
        self._broker = broker_factory(
            settings.bootstrap_servers,
            consumer_only=True,
        )
        subscriber = self._broker.subscriber(
            settings.topic,
            group_id=settings.consumer_group,
            batch=True,
            max_records=settings.max_records,
            batch_timeout_ms=settings.batch_timeout_ms,
            auto_offset_reset="earliest",
            ack_policy=AckPolicy.MANUAL,
        )
        subscriber(self._handle_batch)

    async def start(self) -> None:
        """Подключает broker и запускает FastStream subscriber."""
        await self._broker.start()

    async def stop(self) -> None:
        """Останавливает subscriber и закрывает broker."""
        await self._broker.stop()

    async def _handle_batch(
        self,
        batch: list[TicketPurchasedEvent],
        message: KafkaMessage,
    ) -> None:
        try:
            aggregates = await self._batch_handler(batch)
        except Exception:
            await message.nack()
            logger.exception("Не удалось обработать Kafka-батч; offsets не подтверждены")
            return

        await message.ack()
        await self._payment_activity_queue.put(aggregates)
