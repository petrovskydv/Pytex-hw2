import asyncio
from typing import Any

import pytest
from pydantic import ValidationError
from starlette.routing import WebSocketRoute
from starlette.websockets import WebSocketState

import monitoring.main as monitoring_main
from monitoring.config import WebSocketSettings
from monitoring.domain.dto import PaymentActivityAggregate
from monitoring.services.websocket_delivery import (
    PaymentActivityWebSocketWorker,
    WebSocketConnectionManager,
)


class FakeWebSocket:
    def __init__(
        self,
        *,
        send_delay_seconds: float = 0,
        send_error: Exception | None = None,
    ) -> None:
        self.send_delay_seconds = send_delay_seconds
        self.send_error = send_error
        self.client_state = WebSocketState.CONNECTED
        self.application_state = WebSocketState.CONNECTED
        self.accepted = False
        self.sent_messages: list[dict[str, Any]] = []
        self.send_started = asyncio.Event()

    async def accept(self) -> None:
        self.accepted = True

    async def send_json(self, message: dict[str, Any]) -> None:
        self.send_started.set()
        if self.send_delay_seconds:
            await asyncio.sleep(self.send_delay_seconds)
        if self.send_error is not None:
            raise self.send_error
        self.sent_messages.append(message)


def make_aggregate(event_id: int = 3) -> PaymentActivityAggregate:
    return PaymentActivityAggregate(
        event_id=event_id,
        payments_count=2,
        tickets_count=6,
        total_amount=12000,
    )


def test_payments_websocket_route_is_registered() -> None:
    """Проверяет регистрацию обязательного WebSocket endpoint /ws/payments."""
    assert any(
        isinstance(route, WebSocketRoute) and route.path == "/ws/payments" for route in monitoring_main.app.routes
    )


def test_websocket_timeout_cannot_exceed_two_seconds() -> None:
    """Проверяет ограничение настройки: таймаут отправки клиенту не может быть больше двух секунд."""
    with pytest.raises(ValidationError):
        WebSocketSettings(send_timeout_seconds=2.1)


@pytest.mark.asyncio
async def test_broadcast_sends_to_clients_concurrently_and_keeps_timeout_client() -> None:
    """Проверяет конкурентную рассылку и сохранение клиента в списке после таймаута отправки."""
    manager = WebSocketConnectionManager()
    slow = FakeWebSocket(send_delay_seconds=0.2)
    fast = FakeWebSocket()

    await manager.connect(slow)
    await manager.connect(fast)

    broadcast_task = asyncio.create_task(manager.broadcast([make_aggregate()], timeout_seconds=0.05))
    await asyncio.wait_for(fast.send_started.wait(), timeout=0.02)
    await broadcast_task

    assert fast.sent_messages
    assert slow.sent_messages == []
    assert manager.active_count == 2


@pytest.mark.asyncio
async def test_broadcast_removes_client_with_closed_connection_error() -> None:
    """Проверяет удаление клиента только после ошибки, когда WebSocket уже находится в состоянии DISCONNECTED."""
    manager = WebSocketConnectionManager()
    closed = FakeWebSocket(send_error=RuntimeError("closed"))
    await manager.connect(closed)
    closed.application_state = WebSocketState.DISCONNECTED

    await manager.broadcast([make_aggregate()], timeout_seconds=0.05)

    assert manager.active_count == 0


@pytest.mark.asyncio
async def test_websocket_worker_reads_queue_and_sends_saved_aggregates() -> None:
    """Проверяет чтение сохранённых агрегатов из asyncio.Queue и отправку требуемого WebSocket payload."""
    queue: asyncio.Queue[list[PaymentActivityAggregate]] = asyncio.Queue()
    manager = WebSocketConnectionManager()
    client = FakeWebSocket()
    await manager.connect(client)
    worker = PaymentActivityWebSocketWorker(queue, manager, send_timeout_seconds=0.05)
    worker.start()

    try:
        await queue.put([make_aggregate()])
        await asyncio.wait_for(client.send_started.wait(), timeout=1)
        await asyncio.wait_for(queue.join(), timeout=1)
    finally:
        await worker.stop()

    assert client.sent_messages == [
        {
            "type": "payment_activity",
            "items": [
                {
                    "event_id": 3,
                    "payments_count": 2,
                    "tickets_count": 6,
                    "total_amount": 12000,
                }
            ],
        }
    ]


@pytest.mark.asyncio
async def test_websocket_worker_drains_queue_before_shutdown() -> None:
    """Проверяет graceful shutdown: worker отправляет остаток asyncio.Queue перед остановкой."""
    queue: asyncio.Queue[list[PaymentActivityAggregate]] = asyncio.Queue()
    manager = WebSocketConnectionManager()
    client = FakeWebSocket(send_delay_seconds=0.02)
    await manager.connect(client)
    worker = PaymentActivityWebSocketWorker(queue, manager, send_timeout_seconds=0.05)
    worker.start()

    await queue.put([make_aggregate()])
    await worker.stop()

    assert queue.empty()
    assert client.sent_messages
