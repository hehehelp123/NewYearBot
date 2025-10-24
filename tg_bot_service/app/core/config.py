import logging
from typing import List
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    BOT_TOKEN: str
    KAFKA_BOOTSTRAP_SERVERS: str
    ORCHESTRATOR_URL: str

    MINIO_URL: str = "minio:9000"
    MINIO_ROOT_USER: str
    MINIO_ROOT_PASSWORD: str
    MINIO_BUCKET: str

    WIFI_PASSWORD: str = "YOUR_WIFI_PASSWORD"
    ADMIN_TELEGRAM_IDS: List[int] = []

    model_config = SettingsConfigDict(env_file=".env")

    @property
    def ADMINS_MAP(self) -> dict:
        ids = self.ADMIN_TELEGRAM_IDS
        names = ["Миша", "Алина"]

        admins = {}
        for i, admin_id in enumerate(ids):
            if admin_id:
                name = names[i] if i < len(names) else f"Admin {i + 1}"
                admins[name] = admin_id

        if not admins:
            logging.warning("ADMIN_TELEGRAM_IDS не задан в .env или пуст. Кнопка 'Админы' не будет работать корректно.")
            return {"Администратор": 1}

        return admins


settings = Settings()