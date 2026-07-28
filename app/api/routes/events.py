from fastapi import APIRouter

from app.api.routes.dependencies import CurrentUserId
from app.api.schemas import BookingCreate, CheckoutResponse, EventRead, EventSeatRead

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
) -> CheckoutResponse:
    """Временно бронирует места за клиентом и возвращает расчет checkout."""
    # TODO: проверить что событие и места существуют, если нет вернуть ошибку
    # TODO:
    #  1. выбрать все не забронированные места из EventSeat по списку нужных мест
    #  2. проверить что получены все нужные места, если нет вернуть ошибку
    #  3. создать бронь в Booking, EventSeat

    # TODO: создать бронь для выбранных мест через SELECT FOR UPDATE, и посчитать базовую стоимость.
    # TODO: конкурентно запросить Payment API и Protection API для расчета checkout.
    ...
