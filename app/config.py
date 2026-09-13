from pathlib import Path

from pydantic import BaseModel, HttpUrl, PositiveFloat, PositiveInt, PostgresDsn, RedisDsn, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseModel):
    """Настройки подключения к базе данных."""

    url: PostgresDsn
    """URL подключения к PostgreSQL."""


class RedisSettings(BaseModel):
    """Настройки подключения к Redis."""

    url: RedisDsn
    """URL подключения к Redis."""

    socket_timeout_seconds: PositiveFloat = 1
    """Таймаут операций с Redis в секундах."""


class ExternalApiSettings(BaseModel):
    """Адреса внешних API."""

    payment_api_url: HttpUrl
    """Базовый URL API платежей."""

    protection_api_url: HttpUrl
    """Базовый URL API страховой защиты."""


class BookingSettings(BaseModel):
    """Настройки бронирования."""

    booking_ttl_minutes: PositiveInt
    """Время действия брони в минутах."""


class EventReadSettings(BaseModel):
    """Настройки чтения мероприятий с кэшированием."""

    cache_ttl_seconds: PositiveInt = 300
    """Время хранения мероприятия в кэше в секундах."""

    lock_ttl_seconds: PositiveInt = 15
    """Время жизни распределённой блокировки в секундах."""

    database_timeout_seconds: PositiveFloat = 5
    """Таймаут загрузки мероприятия из базы данных в секундах."""

    lock_wait_seconds: PositiveFloat = 1
    """Максимальное ожидание заполнения кэша лидером в секундах."""


class EventViewSettings(BaseModel):
    """Настройки батчинга просмотров мероприятий."""

    batch_size: PositiveInt = 10
    """Количество событий для сброса в базу данных."""

    flush_seconds: PositiveFloat = 5
    """Максимальное время ожидания перед сбросом батча в секундах."""


class TaskiqSettings(BaseModel):
    """Настройки фоновых задач TaskIQ."""

    broker_url: RedisDsn | None = None
    """Отдельный Redis URL TaskIQ; по умолчанию используется основной Redis."""

    reports_directory: Path = Path("reports")
    """Каталог для локального сохранения PDF-отчётов."""

    protection_retry_delay_seconds: PositiveFloat = 5
    """Задержка между двумя фоновыми попытками расчёта защиты."""


class KafkaSettings(BaseModel):
    """Настройки Kafka для событий о покупках билетов."""

    bootstrap_servers: str = "localhost:9092"
    """Адрес Kafka broker для подключения producer и consumer."""

    topic: str = "tickets.purchased"
    """Topic доменного события о состоявшейся покупке билетов."""

    linger_ms: PositiveInt = 75
    """Окно накопления сообщений producer перед сетевой отправкой."""

    consumer_group: str = "payment-monitor"
    """Consumer group сервиса мониторинга покупок."""

    max_records: PositiveInt = 10
    """Максимальное число событий в одном batch."""

    batch_timeout_ms: PositiveInt = 500
    """Максимальное ожидание неполного batch в миллисекундах."""


class PurchaseGeneratorSettings(BaseModel):
    """Настройки фонового генератора тестовых покупок."""

    interval_seconds: PositiveFloat = 0.05
    """Пауза между покупками; 50 мс создают до десяти событий за 500 мс."""

    event_id_max: PositiveInt = 5
    """Верхняя граница небольшого диапазона event_id, начиная с 1."""


class Settings(BaseSettings):
    """Конфигурация приложения."""

    model_config = SettingsConfigDict(env_file=".env", env_nested_delimiter="__", extra="ignore")

    database: DatabaseSettings
    """Настройки базы данных."""

    redis: RedisSettings
    """Настройки Redis."""

    external_apis: ExternalApiSettings
    """Адреса внешних API."""

    booking: BookingSettings
    """Настройки бронирования."""

    event_read: EventReadSettings = EventReadSettings()
    """Настройки чтения мероприятий."""

    event_view: EventViewSettings = EventViewSettings()
    """Настройки батчинга просмотров."""

    taskiq: TaskiqSettings = TaskiqSettings()
    """Настройки фоновых задач TaskIQ."""

    kafka: KafkaSettings = KafkaSettings()
    """Настройки Kafka и потока событий о покупках."""

    purchase_generator: PurchaseGeneratorSettings = PurchaseGeneratorSettings()
    """Настройки фонового генератора тестовых покупок."""

    @model_validator(mode="after")
    def validate_event_lock_timeout(self) -> "Settings":
        """Проверяет, что блокировка не истечёт до завершения загрузки."""
        if (
            self.event_read.lock_ttl_seconds
            <= self.event_read.database_timeout_seconds + 4 * self.redis.socket_timeout_seconds
        ):
            raise ValueError("EVENT_LOCK_TTL_SECONDS must cover database and Redis operation timeouts")
        return self


settings = Settings()
