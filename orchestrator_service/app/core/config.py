from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    KAFKA_BOOTSTRAP_SERVERS: str
    USER_SERVICE_URL: str
    WISHLIST_SERVICE_URL: str
    TICKET_SERVICE_URL: str
    MUSIC_UPLOADER_SERVICE_URL: str

    MINIO_URL: str = "minio:9000"
    MINIO_ROOT_USER: str
    MINIO_ROOT_PASSWORD: str
    MINIO_BUCKET: str

    model_config = SettingsConfigDict(env_file=".env")

settings = Settings()