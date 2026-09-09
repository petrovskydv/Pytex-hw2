import asyncio

import pytest
from starlette.websockets import WebSocketState

from monitoring.domain.dto import PaymentActivityAggregate
from monitoring.services.websocket_delivery import PaymentActivityWebSocketWorker, WebSocketConnectionManager


class DelayedWebSocket:
    def __init__(self) -> None:
        self.client_state = WebSocketState.CONNECTED
        self.application_state = WebSocketState.CONNECTED
        self.sent = False

    async def accept(self) -> None:
        pass

    async def send_json(self, message: object) -> None:
        await asyncio.sleep(0.02)
        self.sent = True


@pytest.mark.asyncio
async def test_websocket_worker_drains_queue_before_shutdown() -> None:
    queue: asyncio.Queue[list[PaymentActivityAggregate]] = asyncio.Queue()
    manager = WebSocketConnectionManager()
    client = DelayedWebSocket()
    await manager.connect(client)
    worker = PaymentActivityWebSocketWorker(queue, manager, send_timeout_seconds=0.05)
    worker.start()

    await queue.put(
        [
            PaymentActivityAggregate(
                event_id=3,
                payments_count=2,
                tickets_count=6,
                total_amount=12000,
            )
        ]
    )
    await worker.stop()

    assert queue.empty()
    assert client.sent is True
