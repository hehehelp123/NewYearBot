from fastapi import APIRouter

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
                    "kafka_topic": "music.sync.start",
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