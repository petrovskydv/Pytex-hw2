import asyncio
import logging
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

from aiokafka import AIOKafkaConsumer

from app.domain.dto import TicketPurchasedEvent
from monitoring.config import KafkaSettings
from monitoring.domain.dto import PaymentActivityAggregate

logger = logging.getLogger(__name__)

PurchaseBatchHandler = Callable[[list[TicketPurchasedEvent]], Awaitable[list[PaymentActivityAggregate]]]


@dataclass(slots=True)
class PurchaseKafkaBatch:
    """Kafka-батч с событиями и границами offsets по partition."""

    events: list[TicketPurchasedEvent]
    commit_offsets: dict[Any, int]
    retry_offsets: dict[Any, int]


def deserialize_ticket_purchase(value: bytes) -> TicketPurchasedEvent:
    """Преобразует Kafka payload в событие о покупке билетов."""
    return TicketPurchasedEvent.model_validate_json(value)


async def collect_purchase_batch(
    consumer: Any,
    *,
    max_records: int,
    batch_timeout_ms: int,
) -> PurchaseKafkaBatch:
    """Собирает до max_records событий и offsets не дольше batch_timeout_ms."""
    events: list[TicketPurchasedEvent] = []
    commit_offsets: dict[Any, int] = {}
    retry_offsets: dict[Any, int] = {}
    loop = asyncio.get_running_loop()
    deadline = loop.time() + batch_timeout_ms / 1000

    while len(events) < max_records:
        remaining_seconds = deadline - loop.time()
        if remaining_seconds <= 0:
            break

        records_by_partition = await consumer.getmany(
            timeout_ms=max(1, int(remaining_seconds * 1000)),
            max_records=max_records - len(events),
        )
        records_received = 0
        for partition, records in records_by_partition.items():
            if not records:
                continue

            retry_offsets.setdefault(partition, records[0].offset)
            commit_offsets[partition] = records[-1].offset + 1
            events.extend(record.value for record in records)
            records_received += len(records)

        if records_received == 0:
            break

    return PurchaseKafkaBatch(
        events=events,
        commit_offsets=commit_offsets,
        retry_offsets=retry_offsets,
    )


class MonitoringKafka:
    """Управляет Kafka consumer и фоновым чтением батчей покупок."""

    def __init__(
        self,
        settings: KafkaSettings,
        batch_handler: PurchaseBatchHandler,
        payment_activity_queue: asyncio.Queue[list[PaymentActivityAggregate]],
        consumer_factory: Callable[..., Any] = AIOKafkaConsumer,
    ) -> None:
        self._settings = settings
        self._batch_handler = batch_handler
        self._payment_activity_queue = payment_activity_queue
        self._consumer_factory = consumer_factory
        self._consumer: Any | None = None
        self._worker_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Подключает consumer к Kafka и запускает чтение батчей."""
        if self._consumer is not None:
            return

        consumer = self._consumer_factory(
            self._settings.topic,
            bootstrap_servers=self._settings.bootstrap_servers,
            group_id=self._settings.consumer_group,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            value_deserializer=deserialize_ticket_purchase,
        )
        self._consumer = consumer
        try:
            await consumer.start()
            self._worker_task = asyncio.create_task(self._consume_batches(), name="purchase-batch-consumer")
        except BaseException:
            self._consumer = None
            self._worker_task = None
            await consumer.stop()
            raise

    async def stop(self) -> None:
        """Останавливает чтение батчей и закрывает Kafka consumer."""
        worker_task = self._worker_task
        consumer = self._consumer
        self._worker_task = None
        self._consumer = None

        try:
            if worker_task is not None:
                worker_task.cancel()
                with suppress(asyncio.CancelledError):
                    await worker_task
        finally:
            if consumer is not None:
                await consumer.stop()

    async def _consume_batches(self) -> None:
        consumer = self._consumer
        if consumer is None:
            return

        while True:
            batch = await collect_purchase_batch(
                consumer,
                max_records=self._settings.max_records,
                batch_timeout_ms=self._settings.batch_timeout_ms,
            )
            if not batch.events:
                continue

            try:
                aggregates = await self._batch_handler(batch.events)
            except Exception:
                for partition, offset in batch.retry_offsets.items():
                    consumer.seek(partition, offset)
                logger.exception("Не удалось обработать Kafka-батч; offsets не подтверждены")
                continue

            await consumer.commit(batch.commit_offsets)
            await self._payment_activity_queue.put(aggregates)
