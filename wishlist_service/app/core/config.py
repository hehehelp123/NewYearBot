from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    WISHLIST_DATABASE_URL: str
    KAFKA_BOOTSTRAP_SERVERS: str
    SELENIUM_URL_WISHLIST: str

    model_config = SettingsConfigDict(env_file=".env")

settings = Settings()