from fastapi import APIRouter, HTTPException, Request, status

from app.api.dependencies import CheckoutServiceDeps, CurrentUserId, EventReadServiceDeps
from app.api.schemas import (
    BookingCreate,
    CheckoutBooking,
    CheckoutResponse,
    EventRead,
    EventSeatRead,
)
from app.domain.exceptions import (
    EventCacheUnavailableError,
    EventLoadTimeoutError,
    NotFoundError,
    PaymentCalculationError,
    SeatsUnavailableError,
)

router = APIRouter(prefix="/events", tags=["events"])

PREPARE_CHECKOUT_RESPONSES = {
    status.HTTP_404_NOT_FOUND: {
        "description": "Мероприятие или выбранные места не найдены",
        "content": {
            "application/json": {
                "examples": {
                    "event_not_found": {"value": {"detail": "Event not found"}},
                    "seats_not_found": {"value": {"detail": "Selected seats not found"}},
                }
            }
        },
    },
    status.HTTP_409_CONFLICT: {
        "description": "Хотя бы одно выбранное место недоступно",
        "content": {"application/json": {"example": {"detail": "Selected seats are unavailable"}}},
    },
    status.HTTP_503_SERVICE_UNAVAILABLE: {
        "description": "Не удалось рассчитать стоимость оплаты",
        "content": {"application/json": {"example": {"detail": "PaymentDeps calculation is unavailable"}}},
    },
}

GET_EVENT_RESPONSES = {
    status.HTTP_404_NOT_FOUND: {
        "description": "Мероприятие с указанным идентификатором не найдено",
        "content": {"application/json": {"example": {"detail": "Event not found"}}},
    },
    status.HTTP_503_SERVICE_UNAVAILABLE: {
        "description": "Сервис временно недоступен",
        "content": {"application/json": {"example": {"detail": "Event loading is unavailable"}}},
    },
}


@router.get("")
async def list_events() -> list[EventRead]:
    """Возвращает список мероприятий для клиента."""
    ...


@router.get(
    "/{event_id}",
    summary="Получение мероприятия",
    description="Возвращает описание мероприятия. Все денежные суммы указаны в копейках.",
    response_description="Описание мероприятия",
    responses=GET_EVENT_RESPONSES,
)
async def get_event(
    event_id: int,
    request: Request,
    event_service: EventReadServiceDeps,
) -> EventRead:
    """Возвращает описание мероприятия."""
    try:
        event = EventRead.model_validate(await event_service.get_event(event_id, request.client.host))
    except NotFoundError as error:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=error.detail) from None
    except (EventCacheUnavailableError, EventLoadTimeoutError):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Event loading is unavailable",
        ) from None
    return event


@router.get("/{event_id}/seats")
async def list_event_seats(event_id: int) -> list[EventSeatRead]:
    """Возвращает места на мероприятии с ценами и статусами."""
    ...


@router.post(
    "/{event_id}/checkout",
    summary="Бронирование мест",
    description=(
        "Резервирует выбранные места на определенное время и возвращает стоимость оплаты. "
        "Все суммы указаны в копейках. Расчет защиты может отсутствовать, если сервис "
        "защиты недоступен или защита недоступна для мероприятия."
    ),
    responses=PREPARE_CHECKOUT_RESPONSES,
)
async def prepare_checkout(
    event_id: int,
    payload: BookingCreate,
    user_id: CurrentUserId,
    checkout_service: CheckoutServiceDeps,
) -> CheckoutResponse:
    """Временно бронирует места за клиентом и возвращает расчет checkout."""
    try:
        checkout = await checkout_service.create_checkout_booking(event_id, user_id, payload.seat_ids)
    except NotFoundError as error:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=error.detail) from None
    except SeatsUnavailableError:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Selected seats are unavailable") from None
    except PaymentCalculationError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
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
        payment=checkout.payment.model_dump(),
        protection=checkout.protection.model_dump() if checkout.protection else None,
    )
