import asyncio

import httpx

MAX_CONCURRENT_REQUESTS = 10


class BaseApiClient:
    def __init__(self, http_client: httpx.AsyncClient, base_url: str) -> None:
        self._http_client = http_client
        self._base_url = base_url.rstrip("/")
        self._semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async def _post(self, endpoint: str, payload: dict[str, object]) -> httpx.Response:
        async with self._semaphore:
            return await self._http_client.post(f"{self._base_url}{endpoint}", json=payload)
