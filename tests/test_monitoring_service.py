import asyncio
from types import SimpleNamespace
from typing import Any

import pytest
from starlette.routing import WebSocketRoute

import monitoring.main as monitoring_main
from app.domain.dto import TicketPurchasedEvent
from monitoring.config import KafkaSettings, WebSocketSettings
from monitoring.domain.dto import PaymentActivityAggregate
from monitoring.infrastructure.kafka import MonitoringKafka, deserialize_ticket_purchase
from monitoring.main import create_app
from monitoring.services.websocket_delivery import WebSocketConnectionManager


class FakeResource:
    def __init__(self, name: str, calls: list[str]) -> None:
        self.name = name
        self.calls = calls

    async def start(self) -> None:
        self.calls.append(f"{self.name}.start")

    async def stop(self) -> None:
        self.calls.append(f"{self.name}.stop")


class FakeEngine:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls

    async def dispose(self) -> None:
        self.calls.append("database.dispose")


class FailingKafka(FakeResource):
    async def start(self) -> None:
        self.calls.append("kafka.start")
        raise RuntimeError("Kafka unavailable")


class FakeConsumer:
    def __init__(self, captured: dict[str, Any]) -> None:
        self.captured = captured

    async def start(self) -> None:
        self.captured["started"] = True

    async def stop(self) -> None:
        self.captured["stopped"] = True

    async def getmany(self, **kwargs: Any) -> dict[Any, list[Any]]:
        self.captured["getmany_kwargs"] = kwargs
        await asyncio.Event().wait()
        return {}


class FakeConsumerFactory:
    def __init__(self, captured: dict[str, Any]) -> None:
        self.captured = captured

    def __call__(self, *args: Any, **kwargs: Any) -> FakeConsumer:
        self.captured["args"] = args
        self.captured["kwargs"] = kwargs
        return FakeConsumer(self.captured)


async def ignore_purchase_batch(batch: list[TicketPurchasedEvent]) -> list[PaymentActivityAggregate]:
    """Обработчик-заглушка для unit-тестов Kafka lifecycle."""
    return []


def make_settings() -> SimpleNamespace:
    return SimpleNamespace(
        kafka=KafkaSettings(),
        websocket=WebSocketSettings(),
    )


@pytest.mark.asyncio
async def test_monitoring_lifespan_owns_resources(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    settings = make_settings()
    kafka = FakeResource("kafka", calls)
    monkeypatch.setattr(monitoring_main, "engine", FakeEngine(calls))

    app = create_app(
        settings_factory=lambda: settings,
        kafka_factory=lambda _settings, _handler, _queue: kafka,
    )

    async with app.router.lifespan_context(app):
        assert app.state.kafka is kafka
        assert isinstance(app.state.payment_activity_queue, asyncio.Queue)
        assert isinstance(app.state.websocket_manager, WebSocketConnectionManager)
        assert app.state.websocket_worker is not None
        assert calls == ["kafka.start"]

    assert calls == ["kafka.start", "kafka.stop", "database.dispose"]


@pytest.mark.asyncio
async def test_monitoring_lifespan_cleans_up_after_kafka_start_error(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    settings = make_settings()
    kafka = FailingKafka("kafka", calls)
    monkeypatch.setattr(monitoring_main, "engine", FakeEngine(calls))
    app = create_app(
        settings_factory=lambda: settings,
        kafka_factory=lambda _settings, _handler, _queue: kafka,
    )

    with pytest.raises(RuntimeError, match="Kafka unavailable"):
        async with app.router.lifespan_context(app):
            pass

    assert calls == ["kafka.start", "kafka.stop", "database.dispose"]


@pytest.mark.asyncio
async def test_kafka_connection_disables_auto_commit() -> None:
    captured: dict[str, Any] = {}
    connection = MonitoringKafka(
        KafkaSettings(),
        batch_handler=ignore_purchase_batch,
        payment_activity_queue=asyncio.Queue(),
        consumer_factory=FakeConsumerFactory(captured),
    )

    await connection.start()
    await asyncio.sleep(0)
    await connection.stop()

    assert captured["args"] == ("tickets.purchased",)
    assert captured["kwargs"]["bootstrap_servers"] == "localhost:9092"
    assert captured["kwargs"]["group_id"] == "payment-monitor"
    assert captured["kwargs"]["enable_auto_commit"] is False
    assert captured["kwargs"]["value_deserializer"] is deserialize_ticket_purchase
    assert captured["getmany_kwargs"]["max_records"] == 10
    assert 0 < captured["getmany_kwargs"]["timeout_ms"] <= 500
    assert captured["started"] is True
    assert captured["stopped"] is True


def test_payments_websocket_route_is_registered() -> None:
    app = create_app(settings_factory=make_settings)

    assert any(isinstance(route, WebSocketRoute) and route.path == "/ws/payments" for route in app.routes)
