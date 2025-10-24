import os
from typing import List

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str = os.environ.get("DATABASE_URL", "sqlite+aiosqlite:///./user_service.db")
    KAFKA_BOOTSTRAP_SERVERS: str = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    ADMIN_TELEGRAM_IDS: List[int] = []

    KAFKA_CONNECT_RETRIES: int = 15
    KAFKA_CONNECT_RETRY_DELAY: int = 3

    class Config:
        env_file = ".env"
        env_file_encoding = 'utf-8'


settings = Settings()