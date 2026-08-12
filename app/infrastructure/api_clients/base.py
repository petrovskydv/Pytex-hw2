import asyncio

import httpx

RATE_LIMIT_REQUESTS = 5
RATE_LIMIT_INTERVAL_SECONDS = 1


class BaseApiClient:
    def __init__(
        self,
        http_client: httpx.AsyncClient,
        base_url: str,
        rate_limit_requests: int = RATE_LIMIT_REQUESTS,
        rate_limit_interval: float = RATE_LIMIT_INTERVAL_SECONDS,
    ) -> None:
        self._http_client = http_client
        self._base_url = base_url.rstrip("/")
        self._rate_limiter = asyncio.Semaphore(rate_limit_requests)
        self._rate_limit_interval = rate_limit_interval

    async def _release_rate_limiter_later(self) -> None:
        await asyncio.sleep(self._rate_limit_interval)
        self._rate_limiter.release()

    async def _post(self, endpoint: str, payload: dict[str, object]) -> httpx.Response:
        await self._rate_limiter.acquire()
        asyncio.create_task(self._release_rate_limiter_later())
        return await self._http_client.post(f"{self._base_url}{endpoint}", json=payload)
