from functools import lru_cache
from typing import Self

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration loaded from environment variables and `.env`.

    Defaults here target local development on the host (`localhost`) so unit tests
    and CLI tools work without a `.env` file present. Docker Compose overrides
    the hostnames (`postgres`, `rabbitmq`) via `.env` / environment variables.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_name: str = "Async Payment Gateway"
    environment: str = "development"
    log_level: str = "INFO"

    # Checked against the X-API-Key header on all /api/v1/* endpoints.
    api_key: str = Field(default="dev-secret-api-key", min_length=1)

    # SQLAlchemy 2.0 asyncpg DSN.
    database_url: str = Field(
        default="postgresql+asyncpg://postgres:postgres@localhost:5432/payments",
        min_length=1,
    )
    db_echo: bool = False
    db_pool_size: int = Field(default=10, ge=1)
    db_max_overflow: int = Field(default=20, ge=0)

    # RabbitMQ topology: primary exchange & queue `payments.new`,
    # dead-letter exchange `payments.dlx`, dead-letter queue `payments.dlq`.
    rabbitmq_url: str = Field(default="amqp://guest:guest@localhost:5672/", min_length=1)
    rabbitmq_exchange: str = Field(default="payments.new", min_length=1)
    rabbitmq_queue: str = Field(default="payments.new", min_length=1)
    rabbitmq_dlx: str = Field(default="payments.dlx", min_length=1)
    rabbitmq_dlq: str = Field(default="payments.dlq", min_length=1)

    # Outbox relay polling settings.
    outbox_poll_interval_seconds: float = Field(default=1.0, gt=0)
    outbox_batch_size: int = Field(default=50, ge=1)
    outbox_max_retries: int = Field(default=5, ge=1)

    # Simulated gateway: random 2-5s delay, 90% success rate.
    payment_processing_min_seconds: float = Field(default=2.0, ge=0)
    payment_processing_max_seconds: float = Field(default=5.0, ge=0)
    payment_success_rate: float = Field(default=0.9, ge=0.0, le=1.0)
    # Retries AFTER the first attempt. The default 3 gives delays of 2s, 4s, 8s
    # (base * 2^(n-1)) before retries 1-3; if all of them fail, the message goes to the DLQ.
    consumer_max_retries: int = Field(default=3, ge=0)
    consumer_retry_base_delay_seconds: float = Field(default=2.0, gt=0)

    # Outbound webhook: total attempts, not retries. The default 3 sleeps 1s, then 2s.
    webhook_timeout_seconds: float = Field(default=10.0, gt=0)
    webhook_max_retries: int = Field(default=3, ge=1)
    webhook_retry_base_delay_seconds: float = Field(default=1.0, gt=0)

    @model_validator(mode="after")
    def _validate_processing_delay_bounds(self) -> Self:
        if self.payment_processing_min_seconds > self.payment_processing_max_seconds:
            raise ValueError(
                "payment_processing_min_seconds cannot exceed payment_processing_max_seconds"
            )
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
