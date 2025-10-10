from pydantic import BaseModel

class MusicUpload(BaseModel):
    user_id: int
    file_name: str
    artist: str
    title: str