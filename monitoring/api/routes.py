from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter()


@router.websocket("/ws/payments")
async def payments_websocket(websocket: WebSocket) -> None:
    """Поддерживает WebSocket-соединение; рассылка будет добавлена в следующей задаче."""
    await websocket.accept()
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        return
