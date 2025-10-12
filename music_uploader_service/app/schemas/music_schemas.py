from pydantic import BaseModel, HttpUrl

class YandexMusicSyncRequest(BaseModel):
    source_url: HttpUrl
    telegram_id: int