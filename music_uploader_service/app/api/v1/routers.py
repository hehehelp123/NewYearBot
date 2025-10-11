from fastapi import APIRouter
from app.schemas.music_schemas import YandexMusicSyncRequest
from app.services.yandex_music_service import yandex_music_sync_service

router = APIRouter()

@router.get("/features")
def get_music_service_features():
    return [
        {
            "name": "Яндекс.Музыка",
            "type": "menu",
            "items": [
                {
                    "name": "Синхронизировать плейлист/трек",
                    "type": "action",
                    "method": "POST",
                    "url": "/api/v1/music/sync",
                    "payload": {
                        "source_url": {"type": "string", "description": "Ссылка на трек, плейлист или чарт"}
                    }
                }
            ]
        }
    ]

@router.get("/")
def read_root():
    return {"service": "Music Uploader Service", "status": "ok"}

@router.post("/sync/")
async def sync_yandex_music(request: YandexMusicSyncRequest):
    result = await yandex_music_sync_service.sync_from_url(request.source_url)
    return result