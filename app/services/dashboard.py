import asyncio

from app.background.dispatchers import DashboardReportDispatcher
from app.domain.dto import EventDashboardDTO, OccupancyDashboardDTO, SalesDashboardDTO
from app.domain.exceptions import EventNotFoundError
from app.infrastructure.database.db import DatabaseManager


class DashboardService:
    """Собирает аналитику мероприятия для его организатора."""

    def __init__(self, database: DatabaseManager, report_dispatcher: DashboardReportDispatcher) -> None:
        self._database = database
        self._report_dispatcher = report_dispatcher

    async def _get_sales(self, event_id: int) -> SalesDashboardDTO:
        async with self._database.transaction() as database:
            return await database.bookings.get_sales_dashboard(event_id)

    async def _get_occupancy(self, event_id: int) -> OccupancyDashboardDTO:
        async with self._database.transaction() as database:
            return await database.event_seats.get_occupancy_dashboard(event_id)

    async def get_dashboard(self, event_id: int, organizer_id: int) -> EventDashboardDTO:
        # Освобождаем соединение проверки владельца до запуска двух параллельных запросов.
        async with self._database.transaction() as database:
            event = await database.events.get_dashboard_event(event_id, organizer_id)
        if event is None:
            raise EventNotFoundError

        sales, occupancy = await asyncio.gather(self._get_sales(event_id), self._get_occupancy(event_id))
        dashboard = EventDashboardDTO(event=event, sales=sales, occupancy=occupancy)
        await self._report_dispatcher.enqueue(event_id, dashboard)
        return dashboard
