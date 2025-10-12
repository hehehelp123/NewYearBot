from fastapi import APIRouter

router = APIRouter()

@router.get("/features")
def get_user_service_features():
    return [
        {
            "name": "Управление",
            "type": "menu",
            "items": [
                {
                    "name": "Создать нового пользователя",
                    "type": "action",
                    "kafka_topic": "user.user.create",
                    "payload": {
                        "telegram_id": {"type": "integer", "description": "ID пользователя в Telegram"},
                        "username": {"type": "string", "description": "Имя пользователя"}
                    }
                }
            ]
        }
    ]

@router.get("/")
def read_root():
    return {"service": "User Service", "status": "ok"}