class SeatsUnavailableError(Exception):
    """Хотя бы одно из выбранных мест уже недоступно."""


class NotFoundError(Exception):
    """Запрошенный ресурс не найден."""

    detail = "Resource not found"


class EventNotFoundError(NotFoundError):
    """Мероприятие не найдено."""

    detail = "Event not found"


class SeatsNotFoundError(NotFoundError):
    """Хотя бы одно из выбранных мест не найдено для мероприятия."""

    detail = "Selected seats not found"


class PaymentCalculationError(Exception):
    """Не удалось рассчитать стоимость оплаты."""
