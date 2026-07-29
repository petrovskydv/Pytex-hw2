import asyncio

import httpx

from app.infrastructure.payment import PaymentClient
from app.infrastructure.protection import ProtectionClient


async def test_payment_retries_rate_limit(monkeypatch) -> None:
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

    monkeypatch.setattr("app.infrastructure.payment.asyncio.sleep", no_sleep)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        quote = await PaymentClient(http_client, "http://payment").calculate(1, 5000)

    assert calls == 2
    assert quote.commission == 150


async def test_protection_timeout_returns_no_quote(monkeypatch) -> None:
    class SlowHttpClient:
        async def post(self, url: str, json: dict[str, object]) -> httpx.Response:
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

    original_timeout = asyncio.timeout

    def short_timeout(delay: float):
        return original_timeout(0.01)

    monkeypatch.setattr("app.infrastructure.protection.asyncio.timeout", short_timeout)
    quote = await ProtectionClient(SlowHttpClient(), "http://protection").calculate(
        1,
        5000,
        "conference",
        "2030-01-01T00:00:00",
    )

    assert quote is None
