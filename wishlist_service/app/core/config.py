from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    WISHLIST_DATABASE_URL: str
    KAFKA_BOOTSTRAP_SERVERS: str
    SELENIUM_URL_WISHLIST: str
    ALLOWED_WISHLIST_USERS: tuple[int, ...] = (
        574205184, 678863161, 1909727356, 357978239,
        1319347925, 934339168, 1014838346, 921888109
    )

    model_config = SettingsConfigDict(env_file=".env")

settings = Settings()