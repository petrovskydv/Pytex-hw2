from pydantic import BaseModel, HttpUrl, PositiveFloat, PositiveInt, PostgresDsn, RedisDsn, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseModel):
    url: PostgresDsn


class RedisSettings(BaseModel):
    url: RedisDsn
    socket_timeout_seconds: PositiveFloat = 1


class ExternalApiSettings(BaseModel):
    payment_api_url: HttpUrl
    protection_api_url: HttpUrl


class BookingSettings(BaseModel):
    booking_ttl_minutes: PositiveInt
    event_cache_ttl_seconds: PositiveInt = 300
    event_lock_ttl_seconds: PositiveInt = 15
    event_database_timeout_seconds: PositiveFloat = 5
    event_lock_wait_seconds: PositiveFloat = 1


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_nested_delimiter="__", extra="ignore")

    database: DatabaseSettings
    redis: RedisSettings
    external_apis: ExternalApiSettings
    booking: BookingSettings

    @model_validator(mode="after")
    def validate_event_lock_timeout(self) -> "Settings":
        if (
            self.booking.event_lock_ttl_seconds
            <= self.booking.event_database_timeout_seconds + 4 * self.redis.socket_timeout_seconds
        ):
            raise ValueError("EVENT_LOCK_TTL_SECONDS must cover database and Redis operation timeouts")
        return self


settings = Settings()
