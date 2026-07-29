from urllib.parse import quote_plus

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

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore',
    )

    @property
    def consumer_config(self) -> dict:
        return {
            'bootstrap.servers': self.kafka_bootstrap_servers,
            'group.id': 'notifications',
            'enable.auto.commit': 'false',
            'auto.offset.reset': 'smallest'
        }

    @property
    def producer_config(self) -> dict:
        return {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "acks": "all",
        }

    @property
    def DATABASE_URL_ASYNCPG(self):
        user = quote_plus(self.DB_USER)
        password = quote_plus(self.DB_PASS)
        return f"postgresql+asyncpg://{user}:{password}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"


settings = Settings()
