from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from monitoring.services.websocket_delivery import WebSocketConnectionManager

router = APIRouter()


@router.websocket("/ws/payments")
async def payments_websocket(websocket: WebSocket) -> None:
    """Подключает менеджера платформы к потоку агрегированной активности оплат."""
    manager: WebSocketConnectionManager = websocket.app.state.websocket_manager
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(websocket)
