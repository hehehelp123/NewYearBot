from fastapi import APIRouter

router = APIRouter()

@router.get("/features")
def get_ticket_service_features():
    return [
        {
            "name": "Мои билеты",
            "type": "menu",
            "items": [
                {
                    "name": "Добавить билет",
                    "type": "action",
                    "method": "POST",
                    "url": "/api/v1/tickets",
                    "payload": {
                        "title": {"type": "string", "description": "Название поездки (например, 'Москва - Сочи')"},
                        "file": {"type": "file", "description": "PDF файл билета"}
                    }
                },
                {
                    "name": "Посмотреть билеты",
                    "type": "action",
                    "kafka_topic": "ticket.list.request"
                }
            ]
        }
    ]

@router.get("/")
def read_root():
    return {"service": "Ticket Service", "status": "ok"}