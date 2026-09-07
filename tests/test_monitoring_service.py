from types import SimpleNamespace
from typing import Any

import pytest
from starlette.routing import WebSocketRoute

from monitoring.config import KafkaSettings
from monitoring.infrastructure.kafka import MonitoringKafka
from monitoring.main import create_app


class FakeResource:
    def __init__(self, name: str, calls: list[str]) -> None:
        self.name = name
        self.calls = calls

    async def start(self) -> None:
        self.calls.append(f"{self.name}.start")

    async def stop(self) -> None:
        self.calls.append(f"{self.name}.stop")


@pytest.mark.asyncio
async def test_monitoring_lifespan_owns_resources() -> None:
    calls: list[str] = []
    settings = SimpleNamespace(
        database=SimpleNamespace(url="postgresql+psycopg://postgres:postgres@db:5432/postgres"),
        kafka=KafkaSettings(),
    )
    database = FakeResource("database", calls)
    kafka = FakeResource("kafka", calls)

    app = create_app(
        settings_factory=lambda: settings,
        database_factory=lambda _: database,
        kafka_factory=lambda _: kafka,
    )

    async with app.router.lifespan_context(app):
        assert app.state.database is database
        assert app.state.kafka is kafka
        assert calls == ["database.start", "kafka.start"]

    assert calls == ["database.start", "kafka.start", "kafka.stop", "database.stop"]


@pytest.mark.asyncio
async def test_monitoring_lifespan_cleans_up_after_kafka_start_error() -> None:
    calls: list[str] = []
    settings = SimpleNamespace(
        database=SimpleNamespace(url="postgresql+psycopg://postgres:postgres@db:5432/postgres"),
        kafka=KafkaSettings(),
    )
    database = FakeResource("database", calls)

    class FailingKafka(FakeResource):
        async def start(self) -> None:
            self.calls.append("kafka.start")
            raise RuntimeError("Kafka unavailable")

    kafka = FailingKafka("kafka", calls)
    app = create_app(
        settings_factory=lambda: settings,
        database_factory=lambda _: database,
        kafka_factory=lambda _: kafka,
    )

    with pytest.raises(RuntimeError, match="Kafka unavailable"):
        async with app.router.lifespan_context(app):
            pass

    assert calls == ["database.start", "kafka.start", "kafka.stop", "database.stop"]


@pytest.mark.asyncio
async def test_kafka_connection_disables_auto_commit() -> None:
    captured: dict[str, Any] = {}

    class FakeConsumer:
        async def start(self) -> None:
            captured["started"] = True

        async def stop(self) -> None:
            captured["stopped"] = True

    def consumer_factory(*args: Any, **kwargs: Any) -> FakeConsumer:
        captured["args"] = args
        captured["kwargs"] = kwargs
        return FakeConsumer()

    connection = MonitoringKafka(KafkaSettings(), consumer_factory=consumer_factory)
    await connection.start()
    await connection.stop()

    assert captured["args"] == ("tickets.purchased",)
    assert captured["kwargs"]["bootstrap_servers"] == "localhost:9092"
    assert captured["kwargs"]["group_id"] == "payment-monitor"
    assert captured["kwargs"]["enable_auto_commit"] is False
    assert captured["started"] is True
    assert captured["stopped"] is True


def test_payments_websocket_route_is_registered() -> None:
    settings = SimpleNamespace(
        database=SimpleNamespace(url="postgresql+psycopg://postgres:postgres@db:5432/postgres"),
        kafka=KafkaSettings(),
    )
    app = create_app(settings_factory=lambda: settings)

    assert any(isinstance(route, WebSocketRoute) and route.path == "/ws/payments" for route in app.routes)
