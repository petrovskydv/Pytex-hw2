from fastapi import APIRouter, HTTPException, status

from app.api.dependencies import CurrentUserId, DashboardServiceDeps
from app.api.schemas import EventCreate, EventDashboard, EventRead
from app.domain.exceptions import EventNotFoundError

router = APIRouter(prefix="/organizer/events", tags=["organizer events"])

DASHBOARD_RESPONSES = {
    status.HTTP_200_OK: {
        "description": "Аналитика продаж и заполняемости мероприятия",
        "content": {
            "application/json": {
                "example": {
                    "event_title": "Python Conference",
                    "starts_at": "2030-01-01T12:00:00",
                    "sales": {
                        "paid_orders": 42,
                        "sold_tickets": 75,
                        "revenue": 375000,
                        "average_order": 8929,
                    },
                    "occupancy": {
                        "total": 100,
                        "available": 20,
                        "reserved": 5,
                        "sold": 75,
                        "occupancy_percent": 80.0,
                    },
                }
            }
        },
    },
    status.HTTP_404_NOT_FOUND: {
        "description": "Мероприятие не существует или не принадлежит организатору",
        "content": {"application/json": {"example": {"detail": "Event not found"}}},
    },
}


@router.get("")
async def list_organizer_events(organizer_id: CurrentUserId) -> list[EventRead]:
    """Возвращает список созданных событий текущего организатора."""
    ...


@router.post("")
async def create_event(payload: EventCreate, organizer_id: CurrentUserId) -> EventRead:
    """Создает мероприятие от лица текущего организатора."""
    ...


@router.get(
    "/{event_id}/dashboard",
    summary="Дашборд мероприятия",
    description=(
        "Возвращает аналитику продаж и заполняемости мероприятия. Доступна только его организатору; "
        "для отсутствующего или чужого мероприятия возвращается `404`. Все денежные суммы указаны в копейках."
    ),
    response_description="Аналитика мероприятия организатора",
    responses=DASHBOARD_RESPONSES,
)
async def get_event_dashboard(
    event_id: int,
    organizer_id: CurrentUserId,
    dashboard_service: DashboardServiceDeps,
) -> EventDashboard:
    """Возвращает аналитические данные для дашборда по мероприятию."""
    try:
        dashboard = await dashboard_service.get_dashboard(event_id, organizer_id)
    except EventNotFoundError as error:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=error.detail) from None

    occupancy_percent = (
        (dashboard.occupancy.reserved + dashboard.occupancy.sold) / dashboard.occupancy.total * 100
        if dashboard.occupancy.total
        else 0.0
    )
    return EventDashboard(
        event_title=dashboard.event.title,
        starts_at=dashboard.event.starts_at,
        sales=dashboard.sales.model_dump(),
        occupancy={**dashboard.occupancy.model_dump(), "occupancy_percent": occupancy_percent},
    )
