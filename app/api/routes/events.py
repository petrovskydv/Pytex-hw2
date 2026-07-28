from fastapi import APIRouter, HTTPException, status

from app.api.routes.dependencies import CurrentEventService, CurrentUserId
from app.api.schemas import BookingCreate, CheckoutResponse, EventRead, EventSeatRead
from app.domain.exceptions import NotFoundError, SeatsUnavailableError

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
    event_service: CurrentEventService,
) -> CheckoutResponse:
    """Временно бронирует места за клиентом и возвращает расчет checkout."""
    try:
        await event_service.create_checkout_booking(event_id, user_id, payload.seat_ids)
    except NotFoundError as error:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=error.detail) from None
    except SeatsUnavailableError:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Selected seats are unavailable") from None

    # TODO:
    #  3. создать бронь в Booking, EventSeat

    # TODO: создать бронь для выбранных мест через SELECT FOR UPDATE, и посчитать базовую стоимость.
    # TODO: конкурентно запросить Payment API и Protection API для расчета checkout.
    ...
