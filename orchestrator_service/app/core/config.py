from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    KAFKA_BOOTSTRAP_SERVERS: str
    USER_SERVICE_URL: str

    model_config = SettingsConfigDict(env_file=".env")

settings = Settings()