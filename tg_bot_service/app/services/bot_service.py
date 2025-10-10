from app.core.config import settings
from app.core.http_client import http_client
from app.schemas.bot_schemas import StartCommand

class BotService:
    async def handle_start_command(self, payload: StartCommand):
        response = await http_client.client.post(
            f"{settings.ORCHESTRATOR_URL}/api/v1/users/",
            json=payload.model_dump()
        )
        response.raise_for_status()
        return response.json()

bot_service = BotService()