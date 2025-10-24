from fastapi import APIRouter

router = APIRouter()

@router.get("/")
def health_check():
    return {"service": "Music Uploader Service", "status": "ok"}

@router.get("/features")
def get_features():
    return {
        "menu": [
            {
                "name": "🎶 Музыка",
                "type": "menu",
                "items": [
                    {
                        "name": "Синхронизировать музыку",
                        "type": "action",
                        "kafka_topic": "music.sync.start",
                        "payload": {
                            "source_url": {
                                "type": "string",
                                "description": "URL трека или плейлиста"
                            }
                        }
                    }
                ]
            }
        ]
    }