from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    TICKET_DATABASE_URL: str
    KAFKA_BOOTSTRAP_SERVERS: str

    MINIO_URL: str = "minio:9000"
    MINIO_ROOT_USER: str
    MINIO_ROOT_PASSWORD: str
    MINIO_BUCKET: str

    KAFKA_CONNECT_RETRIES: int = 15
    KAFKA_CONNECT_RETRY_DELAY: int = 3

    ADMIN_TELEGRAM_IDS: list[int] = []


    model_config = SettingsConfigDict(env_file=".env")

settings = Settings()