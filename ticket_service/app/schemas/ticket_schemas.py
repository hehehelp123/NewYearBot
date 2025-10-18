from datetime import datetime, date
from pydantic import BaseModel, ConfigDict

class TicketBase(BaseModel):
    requester_user_id: int
    title: str

class TicketCreate(TicketBase):
    pass

class TicketDetails(TicketBase):
    ticket_id: int
    status: str
    storage_key: str | None = None
    created_at: datetime

    passenger_name: str | None = None
    train_number: str | None = None
    wagon_number: str | None = None
    seat_number: str | None = None
    departure_station: str | None = None
    departure_datetime: datetime | None = None
    arrival_station: str | None = None
    arrival_datetime: datetime | None = None

    model_config = ConfigDict(from_attributes=True)