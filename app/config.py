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


class Settings(BaseSettings):
    """Конфигурация приложения."""

    model_config = SettingsConfigDict(env_file=".env", env_nested_delimiter="__", extra="ignore")

    database: DatabaseSettings
    """Настройки базы данных."""

    redis: RedisSettings
    """Настройки Redis."""

    external_apis: ExternalApiSettings
    """Настройки внешних API."""

    booking: BookingSettings
    """Настройки бронирования."""

    event_read: EventReadSettings
    """Настройки чтения мероприятий."""

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
