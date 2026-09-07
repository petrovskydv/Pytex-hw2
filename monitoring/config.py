from functools import lru_cache

from pydantic import BaseModel, PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseModel):
    """Monitoring PostgreSQL settings."""

    url: PostgresDsn


class KafkaSettings(BaseModel):
    """Monitoring Kafka settings."""

    bootstrap_servers: str = "localhost:9092"
    topic: str = "tickets.purchased"
    consumer_group: str = "payment-monitor"


class MonitoringSettings(BaseSettings):
    """Configuration of the purchase monitoring service."""

    model_config = SettingsConfigDict(env_file=".env", env_nested_delimiter="__", extra="ignore")

    database: DatabaseSettings
    kafka: KafkaSettings = KafkaSettings()


@lru_cache
def get_settings() -> MonitoringSettings:
    return MonitoringSettings()
