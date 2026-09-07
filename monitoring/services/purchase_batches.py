import logging

from app.domain.dto import TicketPurchasedEvent

logger = logging.getLogger(__name__)


async def log_purchase_batch(batch: list[TicketPurchasedEvent]) -> None:
    """Фиксирует получение батча до реализации агрегации в задаче 5."""
    logger.info("Получен батч покупок: %s сообщений", len(batch))
