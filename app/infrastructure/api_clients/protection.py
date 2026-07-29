import asyncio

import httpx
from pydantic import ValidationError

from app.domain.dto import ProtectionQuoteDTO


class ProtectionClient:
    def __init__(self, http_client: httpx.AsyncClient, base_url: str) -> None:
        self._http_client = http_client
        self._base_url = base_url.rstrip("/")
        self._semaphore = asyncio.Semaphore(10)

    async def calculate(
        self,
        booking_id: int,
        ticket_amount: int,
        event_category: str,
        event_starts_at: str,
    ) -> ProtectionQuoteDTO | None:
        try:
            async with asyncio.timeout(3):
                async with self._semaphore:
                    response = await self._http_client.post(
                        f"{self._base_url}/protection/calculate",
                        json={
                            "booking_id": booking_id,
                            "ticket_amount": ticket_amount,
                            "event_category": event_category,
                            "event_starts_at": event_starts_at,
                        },
                    )
                response.raise_for_status()
                return ProtectionQuoteDTO.model_validate(response.json())
        except (TimeoutError, httpx.HTTPError, ValidationError):
            return None
