import asyncio
from json import JSONDecodeError

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

from app.domain.dto import ProtectionCalculationDTO
from app.domain.exceptions import ProtectionCalculationError
from app.infrastructure.api_clients.base import BaseApiClient

REQUEST_TIMEOUT_SECONDS = 3
MAX_RETRY_ATTEMPTS = 10
INITIAL_RETRY_DELAY_SECONDS = 0.2
MAX_RETRY_DELAY_SECONDS = 1
RETRY_JITTER_SECONDS = 0.1
RETRIABLE_STATUS_CODES = {500, 503}


class ProtectionClient(BaseApiClient):
    async def calculate_once(
        self,
        booking_id: int,
        ticket_amount: int,
        event_category: str,
        event_starts_at: str,
    ) -> ProtectionCalculationDTO:
        """Выполняет ровно один запрос к сервису страховки."""
        endpoint = "/protection/calculate"
        payload = {
            "booking_id": booking_id,
            "ticket_amount": ticket_amount,
            "event_category": event_category,
            "event_starts_at": event_starts_at,
        }
        try:
            async with asyncio.timeout(REQUEST_TIMEOUT_SECONDS):
                response = await self._post(endpoint, payload)
                response.raise_for_status()
                return ProtectionCalculationDTO.model_validate(response.json())
        except (TimeoutError, httpx.HTTPError, JSONDecodeError, ValidationError) as error:
            raise ProtectionCalculationError from error

    async def calculate(
        self,
        booking_id: int,
        ticket_amount: int,
        event_category: str,
        event_starts_at: str,
    ) -> ProtectionCalculationDTO | None:
        endpoint = "/protection/calculate"
        payload = {
            "booking_id": booking_id,
            "ticket_amount": ticket_amount,
            "event_category": event_category,
            "event_starts_at": event_starts_at,
        }
        try:
            async with asyncio.timeout(REQUEST_TIMEOUT_SECONDS):
                response = await self._retry_post(endpoint, payload)
                response.raise_for_status()
                return ProtectionCalculationDTO.model_validate(response.json())
        except (TimeoutError, httpx.HTTPError, RetryError, JSONDecodeError, ValidationError):
            return None

    @retry(
        retry=(
            retry_if_result(lambda response: response.status_code in RETRIABLE_STATUS_CODES)
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
