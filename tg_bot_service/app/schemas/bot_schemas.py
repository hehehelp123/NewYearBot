from pydantic import BaseModel
from typing import Optional

class HealthCheckResponse(BaseModel):
    status: str 
    message: str 
    bot_id: Optional[int] = None
    username: Optional[str] = None 