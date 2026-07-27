from pydantic import HttpUrl, PositiveInt, PostgresDsn, RedisDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: PostgresDsn
    redis_url: RedisDsn
    payment_api_url: HttpUrl
    protection_api_url: HttpUrl
    booking_ttl_minutes: PositiveInt


settings = Settings()
