from decimal import Decimal
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    service_name: str = "eventflow"
    environment: str = "development"
    log_level: str = "INFO"
    database_url: str = "postgresql+asyncpg://postgres:postgres@postgres:5432/eventflow"
    kafka_bootstrap_servers: str = "kafka:9092"
    redis_url: str = "redis://redis:6379/0"
    request_id_header: str = "X-Request-ID"
    correlation_id_header: str = "X-Correlation-ID"
    metrics_path: str = "/metrics"
    rate_limit_per_minute: int = 120
    rate_limit_window_seconds: int = 60
    outbox_poll_interval_seconds: float = 1.0
    # Simulated payment provider: orders above this amount are declined.
    payment_max_amount: Decimal = Decimal("100000")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
