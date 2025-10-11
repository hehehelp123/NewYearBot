from pydantic import BaseModel, HttpUrl

class MusicSyncRequest(BaseModel):
    source_url: HttpUrl