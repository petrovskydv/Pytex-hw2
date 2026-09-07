import httpx
from taskiq import TaskiqScheduler
from taskiq.events import TaskiqEvents
from taskiq.middlewares import SmartRetryMiddleware
from taskiq.schedule_sources import LabelScheduleSource
from taskiq.state import TaskiqState
from taskiq_redis import ListRedisScheduleSource, RedisStreamBroker

from app.config import settings

REPORTS_QUEUE = "reports"
CLEANUP_QUEUE = "cleanup"
INSURANCE_QUEUE = "insurance"


def _broker(queue_name: str) -> RedisStreamBroker:
    broker_url = settings.taskiq.broker_url or settings.redis.url
    return RedisStreamBroker(
        str(broker_url),
        queue_name=queue_name,
        consumer_group_name=f"afisha-{queue_name}",
        consumer_id="0",
        unacknowledged_lock_timeout=60_000,
    )


reports_broker = _broker(REPORTS_QUEUE)
cleanup_broker = _broker(CLEANUP_QUEUE)
retry_schedule_source = ListRedisScheduleSource(
    str(settings.taskiq.broker_url or settings.redis.url),
    prefix="insurance-retries",
)
insurance_broker = _broker(INSURANCE_QUEUE).with_middlewares(
    SmartRetryMiddleware(
        # В TaskIQ 0.12 лимит 2 означает исходный запуск + один повтор.
        default_retry_count=2,
        default_delay=settings.taskiq.protection_retry_delay_seconds,
        schedule_source=retry_schedule_source,
    )
)


@insurance_broker.on_event(TaskiqEvents.WORKER_STARTUP)
async def create_protection_http_client(state: TaskiqState) -> None:
    """Создаёт общий HTTP-клиент для задач одного insurance worker."""
    state.protection_http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(connect=3, read=3, write=3, pool=3),
    )


@insurance_broker.on_event(TaskiqEvents.WORKER_SHUTDOWN)
async def close_protection_http_client(state: TaskiqState) -> None:
    """Закрывает пул HTTP-соединений при остановке insurance worker."""
    await state.protection_http_client.aclose()


scheduler = TaskiqScheduler(
    broker=cleanup_broker,
    sources=[LabelScheduleSource(cleanup_broker), retry_schedule_source],
)
