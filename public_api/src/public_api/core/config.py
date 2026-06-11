from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Public API"
    app_host: str = "0.0.0.0"
    app_port: int = 8001

    kafka_bootstrap_servers: str = "kafka:9092"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def producer_config(self) -> dict:
        return {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "acks": "all",
        }


settings = Settings()
