from functools import lru_cache

from pydantic import BaseModel, PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseModel):
    """Настройки PostgreSQL сервиса мониторинга."""

    url: PostgresDsn


class KafkaSettings(BaseModel):
    """Настройки Kafka сервиса мониторинга."""

    bootstrap_servers: str = "localhost:9092"
    topic: str = "tickets.purchased"
    consumer_group: str = "payment-monitor"


class MonitoringSettings(BaseSettings):
    """Конфигурация сервиса мониторинга покупок."""

    model_config = SettingsConfigDict(env_file=".env", env_nested_delimiter="__", extra="ignore")

    database: DatabaseSettings
    kafka: KafkaSettings = KafkaSettings()


@lru_cache
def get_settings() -> MonitoringSettings:
    return MonitoringSettings()
