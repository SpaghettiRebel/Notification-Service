from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    @property
    def producer_config(self):
        config = {
            'bootstrap.servers': '<BOOTSTRAP SERVERS>',
            'sasl.username': '<CLUSTER API KEY>',
            'sasl.password': '<CLUSTER API SECRET>',

            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'acks': 'all'
        }
        return config

    # some kafka data
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
