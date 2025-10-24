from fastapi import APIRouter

router = APIRouter()


@router.get("/features")
def get_features():
    hidden_commands = [
        {
            "name": "Download Ticket",
            "type": "action",
            "kafka_topic": "ticket.download.request"
        },
        {
            "name": "Delete Ticket",
            "type": "action",
            "kafka_topic": "ticket.delete.request"
        }
    ]

    visible_menu = [
        {
            "name": "🎟 Билеты",
            "type": "menu",
            "items": [
                {
                    "name": "Посмотреть билеты",
                    "type": "action",
                    "kafka_topic": "ticket.list.request"
                },
                {
                    "name": "Добавить билет",
                    "type": "action",
                    "kafka_topic": "ticket.create.requested",
                    "payload": {
                        "title": {
                            "description": "Название поездки",
                            "type": "text"
                        },
                        "ticket_file": {
                            "description": "PDF файл билета",
                            "type": "file"
                        }
                    }
                }
            ]
        }
    ]

    return {
        "menu": visible_menu,
        "commands": hidden_commands
    }