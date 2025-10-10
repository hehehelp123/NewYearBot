from datetime import datetime
from pydantic import BaseModel, ConfigDict

class UserBase(BaseModel):
    telegram_id: int
    username: str | None = None

class UserCreate(UserBase):
    pass

class User(UserBase):
    user_id: int
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)