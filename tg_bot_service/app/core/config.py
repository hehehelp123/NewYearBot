from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    BOT_TOKEN: str
    KAFKA_BOOTSTRAP_SERVERS: str
    ORCHESTRATOR_URL: str

    MINIO_URL: str = "minio:9000"
    MINIO_ROOT_USER: str
    MINIO_ROOT_PASSWORD: str
    MINIO_BUCKET: str

    model_config = SettingsConfigDict(env_file=".env")

settings = Settings()