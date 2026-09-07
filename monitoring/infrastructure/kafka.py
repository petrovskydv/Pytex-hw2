from collections.abc import Callable
from typing import Any

from monitoring.config import KafkaSettings


class MonitoringKafka:
    """Own Kafka consumer connection of the monitoring service."""

    def __init__(
        self,
        settings: KafkaSettings,
        consumer_factory: Callable[..., Any] | None = None,
    ) -> None:
        self._settings = settings
        self._consumer_factory = consumer_factory
        self._consumer: Any | None = None

    @staticmethod
    def _default_consumer_factory(*args: Any, **kwargs: Any) -> Any:
        # The monitoring image has aiokafka as a service-specific dependency.
        from aiokafka import AIOKafkaConsumer

        return AIOKafkaConsumer(*args, **kwargs)

    async def start(self) -> None:
        if self._consumer is not None:
            return

        factory = self._consumer_factory or self._default_consumer_factory
        consumer = factory(
            self._settings.topic,
            bootstrap_servers=self._settings.bootstrap_servers,
            group_id=self._settings.consumer_group,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        self._consumer = consumer
        try:
            await consumer.start()
        except BaseException:
            self._consumer = None
            await consumer.stop()
            raise

    async def stop(self) -> None:
        consumer = self._consumer
        self._consumer = None
        if consumer is not None:
            await consumer.stop()
