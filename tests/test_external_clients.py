import asyncio

import httpx
import pytest

from app.domain.exceptions import PaymentCalculationError
from app.infrastructure.api_clients.base import BaseApiClient
from app.infrastructure.api_clients.payment import PaymentClient
from app.infrastructure.api_clients.protection import ProtectionClient


async def test_rate_limiter_limits_request_frequency() -> None:
    """Ограничитель не возвращает токен до завершения интервала."""

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        client = BaseApiClient(http_client, "http://service", rate_limit_requests=1, rate_limit_interval=0.01)
        await client._post("/resource", {})

        started_at = asyncio.get_running_loop().time()
        await client._post("/resource", {})

    assert asyncio.get_running_loop().time() - started_at >= 0.009


async def test_payment_retries_rate_limit(monkeypatch) -> None:
    """Платежный клиент повторяет запрос после ответа 429."""
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(429, request=request)
        return httpx.Response(
            200,
            json={"commission": 150, "total": 5150, "payment_methods": ["bank_card"]},
            request=request,
        )

    async def no_sleep(delay: float) -> None:
        return None

    monkeypatch.setattr("app.infrastructure.api_clients.payment.asyncio.sleep", no_sleep)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        payment_calculation = await PaymentClient(http_client, "http://payment").calculate(1, 5000)

    assert calls == 2
    assert payment_calculation.commission == 150


async def test_payment_retries_network_error(monkeypatch) -> None:
    """Платежный клиент повторяет запрос после сетевой ошибки."""
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ConnectError("payment service unavailable", request=request)
        return httpx.Response(
            200,
            json={"commission": 150, "total": 5150, "payment_methods": ["bank_card"]},
            request=request,
        )

    async def no_sleep(delay: float) -> None:
        return None

    monkeypatch.setattr("app.infrastructure.api_clients.payment.asyncio.sleep", no_sleep)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        payment_calculation = await PaymentClient(http_client, "http://payment").calculate(1, 5000)

    assert calls == 2
    assert payment_calculation.commission == 150


async def test_payment_invalid_json_raises_calculation_error() -> None:
    """Платежный клиент преобразует невалидный JSON в доменную ошибку."""

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="invalid", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        with pytest.raises(PaymentCalculationError):
            await PaymentClient(http_client, "http://payment").calculate(1, 5000)


async def test_protection_retries_temporary_error(monkeypatch) -> None:
    """Клиент защиты повторяет запрос после временной ошибки 503."""
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(503, request=request)
        return httpx.Response(
            200,
            json={"available": True, "price": 350, "covered_amount": 5000},
            request=request,
        )

    async def no_sleep(delay: float) -> None:
        return None

    monkeypatch.setattr("app.infrastructure.api_clients.protection.asyncio.sleep", no_sleep)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        protection_calculation = await ProtectionClient(http_client, "http://protection").calculate(
            1,
            5000,
            "conference",
            "2030-01-01T00:00:00",
        )

    assert calls == 2
    assert protection_calculation is not None
    assert protection_calculation.price == 350


async def test_protection_invalid_json_returns_no_calculation() -> None:
    """Клиент защиты игнорирует невалидный JSON внешнего API."""

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="invalid", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        protection_calculation = await ProtectionClient(http_client, "http://protection").calculate(
            1,
            5000,
            "conference",
            "2030-01-01T00:00:00",
        )

    assert protection_calculation is None


async def test_protection_timeout_returns_no_calculation(monkeypatch) -> None:
    """Клиент защиты возвращает отсутствие расчета при превышении таймаута."""

    async def handler(request: httpx.Request) -> httpx.Response:
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    original_timeout = asyncio.timeout

    def short_timeout(delay: float):
        return original_timeout(0.01)

    monkeypatch.setattr("app.infrastructure.api_clients.protection.asyncio.timeout", short_timeout)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        protection_calculation = await ProtectionClient(http_client, "http://protection").calculate(
            1,
            5000,
            "conference",
            "2030-01-01T00:00:00",
        )

    assert protection_calculation is None
