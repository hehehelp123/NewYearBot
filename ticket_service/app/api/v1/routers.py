from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.db import get_db
from app.schemas.ticket_schemas import Ticket, TicketCreate
from app.services.ticket_service import TicketService

router = APIRouter()

@router.get("/features")
def get_ticket_service_features():
    return [
        {
            "name": "Поддержка",
            "type": "menu",
            "items": [
                {
                    "name": "Создать тикет",
                    "type": "action",
                    "method": "POST",
                    "url": "/api/v1/tickets",
                    "payload": {
                        "requester_user_id": {"type": "integer", "description": "ID пользователя, создающего тикет"},
                        "title": {"type": "string", "description": "Тема обращения"}
                    }
                }
            ]
        }
    ]

@router.get("/")
def read_root():
    return {"service": "Ticket Service", "status": "ok"}

@router.post("/tickets/", response_model=Ticket)
async def create_ticket(ticket: TicketCreate, db: AsyncSession = Depends(get_db)):
    ticket_service = TicketService(db)
    return await ticket_service.create_ticket(ticket)