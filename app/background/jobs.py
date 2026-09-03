from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from app.background.brokers import (
    CLEANUP_QUEUE,
    INSURANCE_QUEUE,
    REPORTS_QUEUE,
    cleanup_broker,
    insurance_broker,
    reports_broker,
)
from app.background.dependencies import ProtectionHttpClient
from app.config import settings
from app.domain.dto import EventDashboardDTO
from app.infrastructure.api_clients.protection import ProtectionClient
from app.infrastructure.database.db import DatabaseManager, session_factory
from app.services.booking_cleanup import BookingCleanupService
from app.services.pdf_reports import generate_event_dashboard_pdf
from app.services.protection_retry import ProtectionRetryService


def _publish_dashboard_report(
    event_id: int,
    dashboard_data: dict[str, object],
    output_directory: Path | None = None,
) -> Path:
    """Создаёт PDF во временном файле и атомарно публикует готовый отчёт."""
    dashboard = EventDashboardDTO.model_validate(dashboard_data)
    generated_at = datetime.now(UTC)
    output_directory = output_directory or settings.taskiq.reports_directory
    output_directory.mkdir(parents=True, exist_ok=True)
    suffix = f"{generated_at:%Y%m%dT%H%M%S%fZ}-{uuid4().hex[:8]}"
    output_path = output_directory / f"dashboard-event-{event_id}-{suffix}.pdf"
    temporary_path = output_path.with_suffix(".pdf.tmp")
    try:
        generate_event_dashboard_pdf(dashboard, temporary_path, generated_at)
        temporary_path.replace(output_path)
    finally:
        temporary_path.unlink(missing_ok=True)
    return output_path


@reports_broker.task(queue_name=REPORTS_QUEUE, ack_type="when_executed")
def generate_dashboard_report_task(event_id: int, dashboard_data: dict[str, object]) -> str:
    """Формирует локальный PDF по снимку данных, возвращённых dashboard."""
    return str(_publish_dashboard_report(event_id, dashboard_data))


@cleanup_broker.task(
    queue_name=CLEANUP_QUEUE,
    ack_type="when_executed",
    schedule=[{"cron": "* * * * *"}],
)
async def cleanup_expired_bookings_task() -> int:
    """Раз в минуту удаляет истёкшие pending-брони и освобождает места."""
    async with session_factory() as session:
        database = DatabaseManager(session, session_factory)
        return await BookingCleanupService(database).cleanup_expired()


@insurance_broker.task(
    queue_name=INSURANCE_QUEUE,
    ack_type="when_executed",
    retry_on_error=True,
    # В TaskIQ 0.12 max_retries=2 даёт исходный запуск и ровно один повтор.
    max_retries=2,
    delay=settings.taskiq.protection_retry_delay_seconds,
)
async def retry_protection_task(
    booking_id: int,
    http_client: ProtectionHttpClient,
) -> bool:
    """Одна попытка расчёта; TaskIQ выполняет не более одного retry (всего две)."""
    async with session_factory() as session:
        database = DatabaseManager(session, session_factory)
        client = ProtectionClient(http_client, settings.external_apis.protection_api_url)
        return await ProtectionRetryService(database, client).calculate_for_pending_booking(booking_id)
