from fastapi import APIRouter

from app.routers.dependencies import CurrentUserId
from app.schemas import PaymentCompleted, PaymentCreate

router = APIRouter(prefix="/bookings", tags=["bookings"])


@router.post("/{booking_id}/pay")
async def pay_booking(
    booking_id: int,
    payload: PaymentCreate,
    user_id: CurrentUserId,
) -> PaymentCompleted:
    """Принимает способ оплаты и флаг with_protection."""
    ...
