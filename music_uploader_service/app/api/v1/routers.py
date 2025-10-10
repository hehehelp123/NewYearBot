from fastapi import APIRouter
from app.schemas.music_schemas import MusicUpload
from app.services.music_service import music_service

router = APIRouter()

@router.get("/")
def read_root():
    return {"service": "Music Uploader Service", "status": "ok"}

@router.post("/upload/")
async def upload_music(upload: MusicUpload):
    return await music_service.process_upload(upload)