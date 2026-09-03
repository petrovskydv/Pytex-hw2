from app.api.schemas import EventDashboard
from app.background.jobs import generate_dashboard_report_task, retry_protection_task


class DashboardReportDispatcher:
    async def enqueue(self, event_id: int, dashboard: EventDashboard) -> None:
        await generate_dashboard_report_task.kiq(event_id, dashboard.model_dump(mode="json"))


class ProtectionRetryDispatcher:
    async def enqueue(self, booking_id: int) -> None:
        await retry_protection_task.kiq(booking_id)
