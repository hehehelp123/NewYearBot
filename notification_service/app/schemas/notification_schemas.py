from datetime import datetime
from pydantic import BaseModel, ConfigDict


class NotificationBase(BaseModel):
    user_id: int
    send_at: datetime
    message: str | None = None


class NotificationCreate(NotificationBase):
    pass


class Notification(NotificationBase):
    id: int
    status: str
    created_at: datetime

    storage_key: str | None = None
    document_caption: str | None = None

    model_config = ConfigDict(from_attributes=True)