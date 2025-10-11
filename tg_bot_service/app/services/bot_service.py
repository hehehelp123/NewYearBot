from app.core.config import settings
from app.core.http_client import http_client
from app.schemas.bot_schemas import HealthCheckResponse
import logging

class BotService:
    async def health_check(self) -> HealthCheckResponse:
        try:
            response = await http_client.client.get(
                f"https://api.telegram.org/bot{settings.BOT_TOKEN}/getMe"
            )
            if response.status_code != 200:
                logging.error(f"Health check failed: Telegram API returned {response.status}")
                return HealthCheckResponse(
                    status="error",
                    message=f"Failed to connect to Telegram API (status: {response.status})"
                )
            
            data = response.json()
            if not data.get("ok") or not data.get("result"):
                logging.error("Health check failed: Invalid response from Telegram API")
                return HealthCheckResponse(
                    status="error",
                    message="Invalid Telegram API response"
                )
            
            logging.info("Bot health check successful")
            return HealthCheckResponse(
                status="healthy",
                message="Bot is operational",
                bot_id=data["result"].get("id"),
                username=data["result"].get("username")
            )
            
        except Exception as e:
            logging.error(f"Health check failed: {str(e)}")
            return HealthCheckResponse(
                status="error",
                message=f"Health check failed: {str(e)}"
            )

bot_service = BotService()