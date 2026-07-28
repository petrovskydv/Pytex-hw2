from fastapi import APIRouter

from app.api.routes.dependencies import CurrentUserId
from app.api.schemas import EventCreate, EventDashboard, EventRead

router = APIRouter(prefix="/organizer/events", tags=["organizer events"])


@router.get("")
async def list_organizer_events(organizer_id: CurrentUserId) -> list[EventRead]:
    """Возвращает список созданных событий текущего организатора."""
    ...


@router.post("")
async def create_event(payload: EventCreate, organizer_id: CurrentUserId) -> EventRead:
    """Создает мероприятие от лица текущего организатора."""
    ...


@router.get("/{event_id}/dashboard")
async def get_event_dashboard(event_id: int, organizer_id: CurrentUserId) -> EventDashboard:
    """Возвращает аналитические данные для дашборда по мероприятию."""
    # TODO: проверить, что мероприятие принадлежит organizer_id.
    # TODO: конкурентно загрузить аналитику продаж и занятость мест отдельными запросами к БД.
    ...
