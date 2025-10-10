from datetime import datetime
from pydantic import BaseModel, ConfigDict

class NotificationBase(BaseModel):
    user_id: int
    message: str
    send_at: datetime

class NotificationCreate(NotificationBase):
    pass

class Notification(NotificationBase):
    notification_id: int
    status: str
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)