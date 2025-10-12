from fastapi import APIRouter, BackgroundTasks
from app.schemas.music_schemas import YandexMusicSyncRequest
from app.services.sync_service import music_sync_service

router = APIRouter()


@router.get("/features")
def get_music_service_features():
    return [
        {
            "name": "Загрузка Музыки",
            "type": "menu",
            "items": [
                {
                    "name": "Синхронизировать по URL",
                    "type": "action",
                    "method": "POST",
                    "url": "/api/v1/music/sync",
                    "payload": {
                        "source_url": {"type": "string", "description": "Ссылка на трек, плейлист или видео"}
                    }
                }
            ]
        }
    ]


@router.get("/")
def read_root():
    return {"service": "Music Uploader Service", "status": "ok"}


@router.post("/sync/")
async def sync_from_url(request: YandexMusicSyncRequest, background_tasks: BackgroundTasks):
    source_url_str = str(request.source_url)

    if "music.yandex.ru" in source_url_str:
        return await music_sync_service._sync_with_yandex_api(source_url_str)
    else:
        background_tasks.add_task(music_sync_service._sync_with_selenium, source_url_str)
        return {"status": "accepted", "detail": "Задача по скачиванию и загрузке принята в фоновую обработку."}