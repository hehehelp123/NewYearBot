from pydantic import BaseModel

class StartCommand(BaseModel):
    telegram_id: int
    username: str | None = None