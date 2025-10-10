from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    NOTIFICATION_DATABASE_URL: str
    KAFKA_BOOTSTRAP_SERVERS: str

    model_config = SettingsConfigDict(env_file=".env")

settings = Settings()