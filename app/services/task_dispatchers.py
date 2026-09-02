from typing import Protocol

from app.api.schemas import EventDashboard


class DashboardReportDispatcher(Protocol):
    async def enqueue(self, event_id: int, dashboard: EventDashboard) -> None: ...


class ProtectionRetryDispatcher(Protocol):
    async def enqueue(self, booking_id: int) -> None: ...
