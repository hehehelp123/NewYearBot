from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    KAFKA_BOOTSTRAP_SERVERS: str
    YANDEX_MUSIC_TOKEN: str
    YANDEX_USER_LOGIN: str
    YANDEX_DESTINATION_PLAYLIST_NAME: str

    model_config = SettingsConfigDict(env_file=".env")

settings = Settings()