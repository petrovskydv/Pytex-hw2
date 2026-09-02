from taskiq import TaskiqScheduler
from taskiq.middlewares import SmartRetryMiddleware
from taskiq.schedule_sources import LabelScheduleSource
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

scheduler = TaskiqScheduler(
    broker=cleanup_broker,
    sources=[LabelScheduleSource(cleanup_broker), retry_schedule_source],
)
