from urllib.parse import quote_plus

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    APP_NAME: str = 'Notification sending service'
    APP_HOST: str = '0.0.0.0'
    APP_PORT: int = 8002

    DB_HOST: str
    DB_PORT: int
    DB_USER: str
    DB_PASS: str
    DB_NAME: str

    kafka_bootstrap_servers: str = 'kafka:9092'
    kafka_delivery_timeout_ms: int = Field(default=10_000, gt=0)
    kafka_max_poll_interval_ms: int = Field(default=180_000, gt=0)
    processing_lease_seconds: int = Field(default=60, gt=0)
    sender_max_attempts: int = Field(default=3, gt=0)
    sender_attempt_timeout_seconds: float = Field(default=10.0, gt=0)
    sender_retry_min_seconds: float = Field(default=1.0, ge=0)
    sender_retry_max_seconds: float = Field(default=5.0, ge=0)

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore',
    )

    @model_validator(mode='after')
    def validate_reliability_timeouts(self) -> 'Settings':
        if self.sender_retry_min_seconds > self.sender_retry_max_seconds:
            raise ValueError(
                'sender_retry_min_seconds must not exceed '
                'sender_retry_max_seconds'
            )

        maximum_processing_seconds = (
            self.sender_max_attempts * self.sender_attempt_timeout_seconds
            + (self.sender_max_attempts - 1) * self.sender_retry_max_seconds
            + self.kafka_delivery_timeout_ms / 1000
            + 2
        )
        if self.processing_lease_seconds <= maximum_processing_seconds:
            raise ValueError(
                'processing_lease_seconds must exceed the maximum sender '
                'retry and DLQ delivery duration'
            )
        if (
            self.kafka_max_poll_interval_ms
            <= self.processing_lease_seconds * 1000
        ):
            raise ValueError(
                'kafka_max_poll_interval_ms must exceed the processing lease'
            )
        return self

    @property
    def consumer_config(self) -> dict:
        return {
            'bootstrap.servers': self.kafka_bootstrap_servers,
            'group.id': 'notifications',
            'enable.auto.commit': 'false',
            'enable.auto.offset.store': 'false',
            'auto.offset.reset': 'earliest',
            'max.poll.interval.ms': self.kafka_max_poll_interval_ms,
        }

    @property
    def producer_config(self) -> dict:
        return {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
            "message.timeout.ms": self.kafka_delivery_timeout_ms,
        }

    @property
    def DATABASE_URL_ASYNCPG(self):
        user = quote_plus(self.DB_USER)
        password = quote_plus(self.DB_PASS)
        return f"postgresql+asyncpg://{user}:{password}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"


settings = Settings()
