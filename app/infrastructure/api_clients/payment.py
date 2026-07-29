import asyncio

import httpx
from pydantic import ValidationError
from tenacity import (
    RetryError,
    retry,
    retry_if_exception_type,
    retry_if_result,
    stop_after_attempt,
    wait_exponential_jitter,
)

from app.domain.dto import PaymentQuoteDTO
from app.domain.exceptions import PaymentCalculationError

MAX_CONCURRENT_REQUESTS = 10
RATE_LIMIT_STATUS_CODE = 429
MAX_RETRY_ATTEMPTS = 10
INITIAL_RETRY_DELAY_SECONDS = 0.2
MAX_RETRY_DELAY_SECONDS = 1
RETRY_JITTER_SECONDS = 0.1


class PaymentClient:
    def __init__(self, http_client: httpx.AsyncClient, base_url: str) -> None:
        self._http_client = http_client
        self._base_url = base_url.rstrip("/")
        self._semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async def calculate(self, booking_id: int, amount: int) -> PaymentQuoteDTO:
        try:
            response = await self._post(booking_id, amount)
            response.raise_for_status()
            return PaymentQuoteDTO.model_validate(response.json())
        except (httpx.HTTPError, RetryError, ValidationError) as error:
            raise PaymentCalculationError from error

    @retry(
        retry=(
            retry_if_result(lambda response: response.status_code == RATE_LIMIT_STATUS_CODE)
            | retry_if_exception_type(httpx.RequestError)
        ),
        stop=stop_after_attempt(MAX_RETRY_ATTEMPTS),
        wait=wait_exponential_jitter(
            initial=INITIAL_RETRY_DELAY_SECONDS,
            max=MAX_RETRY_DELAY_SECONDS,
            jitter=RETRY_JITTER_SECONDS,
        ),
        sleep=asyncio.sleep,
    )
    async def _post(self, booking_id: int, amount: int) -> httpx.Response:
        async with self._semaphore:
            return await self._http_client.post(
                f"{self._base_url}/payment/calculate",
                json={"booking_id": booking_id, "amount": amount, "currency": "RUB"},
            )
