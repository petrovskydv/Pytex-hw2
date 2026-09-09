import asyncio
import logging
from contextlib import suppress
from typing import Any

from fastapi import WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

from monitoring.domain.dto import PaymentActivityAggregate

logger = logging.getLogger(__name__)

PaymentActivityQueue = asyncio.Queue[list[PaymentActivityAggregate]]


def build_payment_activity_message(aggregates: list[PaymentActivityAggregate]) -> dict[str, Any]:
    """Формирует WebSocket-сообщение об агрегированной активности оплат."""
    return {
        "type": "payment_activity",
        "items": [aggregate.model_dump(mode="json") for aggregate in aggregates],
    }


class WebSocketConnectionManager:
    """Хранит активные WebSocket-соединения и рассылает им обновления."""

    def __init__(self) -> None:
        self._connections: set[WebSocket] = set()

    @property
    def active_count(self) -> int:
        """Возвращает количество активных WebSocket-соединений."""
        return len(self._connections)

    async def connect(self, websocket: WebSocket) -> None:
        """Принимает WebSocket-соединение и добавляет его в registry."""
        await websocket.accept()
        self._connections.add(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        """Удаляет WebSocket-соединение из registry."""
        self._connections.discard(websocket)

    async def broadcast(
        self,
        aggregates: list[PaymentActivityAggregate],
        *,
        timeout_seconds: float,
    ) -> None:
        """Конкурентно отправляет агрегаты всем подключённым клиентам."""
        if not self._connections:
            return

        message = build_payment_activity_message(aggregates)
        await asyncio.gather(
            *(
                self._send_to_client(websocket, message, timeout_seconds=timeout_seconds)
                for websocket in tuple(self._connections)
            )
        )

    async def _send_to_client(
        self,
        websocket: WebSocket,
        message: dict[str, Any],
        *,
        timeout_seconds: float,
    ) -> None:
        try:
            async with asyncio.timeout(timeout_seconds):
                await websocket.send_json(message)
        except TimeoutError:
            logger.warning("Таймаут отправки WebSocket-клиенту; соединение сохранено")
        except (WebSocketDisconnect, OSError):
            self.disconnect(websocket)
        except RuntimeError:
            if self._is_closed(websocket):
                self.disconnect(websocket)
            else:
                logger.exception("Ошибка отправки WebSocket-клиенту")
        except Exception:
            if self._is_closed(websocket):
                self.disconnect(websocket)
            else:
                logger.exception("Неожиданная ошибка отправки WebSocket-клиенту")

    @staticmethod
    def _is_closed(websocket: WebSocket) -> bool:
        return (
            websocket.client_state is WebSocketState.DISCONNECTED
            or websocket.application_state is WebSocketState.DISCONNECTED
        )


class PaymentActivityWebSocketWorker:
    """Читает сохранённые агрегаты из asyncio.Queue и рассылает их клиентам."""

    def __init__(
        self,
        queue: PaymentActivityQueue,
        manager: WebSocketConnectionManager,
        send_timeout_seconds: float,
    ) -> None:
        self._queue = queue
        self._manager = manager
        self._send_timeout_seconds = send_timeout_seconds
        self._worker_task: asyncio.Task[None] | None = None

    def start(self) -> None:
        """Запускает фоновую задачу WebSocket-рассылки."""
        if self._worker_task is not None:
            return
        self._worker_task = asyncio.create_task(self._run(), name="payment-activity-websocket-worker")

    async def stop(self) -> None:
        """Дожидается рассылки очереди и останавливает фоновую задачу."""
        worker_task = self._worker_task
        self._worker_task = None
        if worker_task is None:
            return

        await self._queue.join()
        worker_task.cancel()
        with suppress(asyncio.CancelledError):
            await worker_task

    async def _run(self) -> None:
        while True:
            aggregates = await self._queue.get()
            try:
                await self._manager.broadcast(
                    aggregates,
                    timeout_seconds=self._send_timeout_seconds,
                )
            finally:
                self._queue.task_done()
