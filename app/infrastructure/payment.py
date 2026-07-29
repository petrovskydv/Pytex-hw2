import asyncio
import random

import httpx
from pydantic import ValidationError

from app.domain.dto import PaymentQuoteDTO
from app.domain.exceptions import PaymentCalculationError


class PaymentClient:
    def __init__(self, http_client: httpx.AsyncClient, base_url: str) -> None:
        self._http_client = http_client
        self._base_url = base_url.rstrip("/")
        self._semaphore = asyncio.Semaphore(10)

    async def calculate(self, booking_id: int, amount: int) -> PaymentQuoteDTO:
        for attempt in range(3):
            try:
                async with self._semaphore:
                    response = await self._http_client.post(
                        f"{self._base_url}/payment/calculate",
                        json={"booking_id": booking_id, "amount": amount, "currency": "RUB"},
                    )
                if response.status_code == 429 and attempt < 2:
                    await asyncio.sleep(2**attempt + random.uniform(0, 1))
                    continue
                response.raise_for_status()
                return PaymentQuoteDTO.model_validate(response.json())
            except (httpx.HTTPError, ValidationError) as error:
                raise PaymentCalculationError from error

        raise PaymentCalculationError
