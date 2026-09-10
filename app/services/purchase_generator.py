import asyncio
from contextlib import suppress
from datetime import UTC, datetime
from random import randint
from uuid import uuid4

from app.domain.dto import TicketPurchasedEvent
from app.infrastructure.kafka import KafkaPurchasePublisher


class PurchaseEventGenerator:
    """Фоново генерирует тестовые события о состоявшихся покупках."""

    def __init__(
        self,
        publisher: KafkaPurchasePublisher,
        *,
        interval_seconds: float = 0.05,
        event_id_max: int = 5,
    ) -> None:
        self._publisher = publisher
        self._interval_seconds = interval_seconds
        self._event_id_max = event_id_max
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        """Запускает генератор в отдельной asyncio-задаче."""
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._run(), name="purchase-event-generator")

    async def stop(self) -> None:
        """Останавливает генератор и дожидается завершения задачи."""
        task = self._task
        self._task = None
        if task is None:
            return

        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    def create_event(self) -> TicketPurchasedEvent:
        """Создаёт одну тестовую покупку с уникальным payment_id."""
        tickets_count = randint(1, 4)
        ticket_price = randint(10, 50) * 100
        return TicketPurchasedEvent(
            payment_id=uuid4(),
            event_id=randint(1, self._event_id_max),
            tickets_count=tickets_count,
            total_amount=tickets_count * ticket_price,
            paid_at=datetime.now(UTC),
        )

    async def _run(self) -> None:
        while True:
            await self._publisher.publish(self.create_event())
            await asyncio.sleep(self._interval_seconds)
