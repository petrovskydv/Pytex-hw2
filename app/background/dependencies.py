from typing import Annotated

import httpx
from taskiq import Context, TaskiqDepends


def get_protection_http_client(
    context: Annotated[Context, TaskiqDepends()],
) -> httpx.AsyncClient:
    """Возвращает общий HTTP-клиент insurance worker."""
    return context.state.protection_http_client


ProtectionHttpClient = Annotated[httpx.AsyncClient, TaskiqDepends(get_protection_http_client)]
