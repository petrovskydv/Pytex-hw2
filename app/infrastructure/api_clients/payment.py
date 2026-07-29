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

from app.domain.dto import PaymentCalculationDTO
from app.domain.exceptions import PaymentCalculationError
from app.infrastructure.api_clients.base import BaseApiClient

RATE_LIMIT_STATUS_CODE = 429
MAX_RETRY_ATTEMPTS = 10
INITIAL_RETRY_DELAY_SECONDS = 0.2
MAX_RETRY_DELAY_SECONDS = 1
RETRY_JITTER_SECONDS = 0.1


class PaymentClient(BaseApiClient):
    async def calculate(self, booking_id: int, amount: int) -> PaymentCalculationDTO:
        endpoint = "/payment/calculate"
        payload = {"booking_id": booking_id, "amount": amount, "currency": "RUB"}
        try:
            response = await self._retry_post(endpoint, payload)
            response.raise_for_status()
            return PaymentCalculationDTO.model_validate(response.json())
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
    async def _retry_post(self, endpoint: str, payload: dict[str, object]) -> httpx.Response:
        return await self._post(endpoint, payload)
