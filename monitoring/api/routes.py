from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter()


@router.websocket("/ws/payments")
async def payments_websocket(websocket: WebSocket) -> None:
    """Keep a manager connection open; broadcasting is added in task 9."""
    await websocket.accept()
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        return
