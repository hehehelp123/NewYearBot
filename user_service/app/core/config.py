from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List

class Settings(BaseSettings):
    USER_DATABASE_URL: str
    KAFKA_BOOTSTRAP_SERVERS: str
    ADMIN_TELEGRAM_IDS: List[int] = []

    KAFKA_CONNECT_RETRIES: int = 15
    KAFKA_CONNECT_RETRY_DELAY: int = 3

    model_config = SettingsConfigDict(env_file=".env")

settings = Settings()