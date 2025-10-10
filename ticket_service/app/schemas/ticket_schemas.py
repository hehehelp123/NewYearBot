from datetime import datetime
from pydantic import BaseModel, ConfigDict

class TicketBase(BaseModel):
    requester_user_id: int
    title: str
    event_date: datetime | None = None

class TicketCreate(TicketBase):
    pass

class Ticket(TicketBase):
    ticket_id: int
    status: str
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)