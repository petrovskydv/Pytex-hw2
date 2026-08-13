from pydantic import HttpUrl, PositiveFloat, PositiveInt, PostgresDsn, RedisDsn, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: PostgresDsn
    redis_url: RedisDsn
    payment_api_url: HttpUrl
    protection_api_url: HttpUrl
    booking_ttl_minutes: PositiveInt
    redis_socket_timeout_seconds: PositiveFloat = 1
    event_cache_ttl_seconds: PositiveInt = 300
    event_lock_ttl_seconds: PositiveInt = 15
    event_database_timeout_seconds: PositiveFloat = 5
    event_lock_wait_seconds: PositiveFloat = 1

    @model_validator(mode="after")
    def validate_event_lock_timeout(self) -> "Settings":
        if self.event_lock_ttl_seconds <= self.event_database_timeout_seconds + 4 * self.redis_socket_timeout_seconds:
            raise ValueError("EVENT_LOCK_TTL_SECONDS must cover database and Redis operation timeouts")
        return self


settings = Settings()
