from fastapi import APIRouter, HTTPException, status

from app.api.dependencies import CurrentUserId, EventServiceDeps
from app.api.schemas import (
    BookingCreate,
    CheckoutBooking,
    CheckoutResponse,
    EventRead,
    EventSeatRead,
    PaymentQuote,
    ProtectionQuote,
)
from app.domain.exceptions import NotFoundError, PaymentCalculationError, SeatsUnavailableError

router = APIRouter(prefix="/events", tags=["events"])


@router.get("")
async def list_events() -> list[EventRead]:
    """Возвращает список мероприятий для клиента."""
    ...


@router.get("/{event_id}")
async def get_event(event_id: int) -> EventRead:
    """Возвращает описание мероприятия."""
    ...


@router.get("/{event_id}/seats")
async def list_event_seats(event_id: int) -> list[EventSeatRead]:
    """Возвращает места на мероприятии с ценами и статусами."""
    ...


@router.post("/{event_id}/checkout")
async def prepare_checkout(
    event_id: int,
    payload: BookingCreate,
    user_id: CurrentUserId,
    event_service: EventServiceDeps,
) -> CheckoutResponse:
    """Временно бронирует места за клиентом и возвращает расчет checkout."""
    try:
        checkout = await event_service.create_checkout_booking(event_id, user_id, payload.seat_ids)
    except NotFoundError as error:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=error.detail) from None
    except SeatsUnavailableError:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Selected seats are unavailable") from None
    except PaymentCalculationError:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="PaymentDeps calculation is unavailable",
        ) from None

    protection_price = checkout.protection.price if checkout.protection and checkout.protection.available else None
    return CheckoutResponse(
        booking=CheckoutBooking(
            id=checkout.booking.id,
            event_title=checkout.event.title,
            starts_at=checkout.event.starts_at,
            seats=[seat.model_dump() for seat in checkout.seats],
            base_amount=checkout.booking.amount,
            payment_commission=checkout.payment.commission,
            protection_price=protection_price,
            with_protection=False,
            reserved_until=checkout.booking.reserved_until,
        ),
        payment=PaymentQuote.model_validate(checkout.payment.model_dump()),
        protection=ProtectionQuote.model_validate(checkout.protection.model_dump()) if checkout.protection else None,
    )
