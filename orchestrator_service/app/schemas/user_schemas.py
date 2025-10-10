from pydantic import BaseModel

class UserCreateRequest(BaseModel):
    telegram_id: int
    username: str | None = None