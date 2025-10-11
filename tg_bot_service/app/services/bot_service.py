import logging
from app.core.config import settings
from app.core.http_client import http_client
from app.schemas.bot_schemas import HealthCheckResponse

logging.basicConfig(level=logging.INFO)


class BotService:
    async def health_check(self) -> HealthCheckResponse:
        try:
            response = await http_client.client.get(
                f"https://api.telegram.org/bot{settings.BOT_TOKEN}/getMe"
            )
            response.raise_for_status()

            data = response.json()
            if not data.get("ok") or not data.get("result"):
                logging.error("Health check failed: Invalid response from Telegram API")
                return HealthCheckResponse(status="error", message="Invalid Telegram API response")

            logging.info("Bot health check successful")
            return HealthCheckResponse(
                status="healthy",
                message="Bot is operational",
                bot_id=data["result"].get("id"),
                username=data["result"].get("username")
            )

        except Exception as e:
            logging.error(f"Health check failed: {str(e)}")
            return HealthCheckResponse(status="error", message=f"Health check failed: {str(e)}")

    async def execute_action(self, action: dict, collected_data: dict) -> dict:
        url = f"{settings.ORCHESTRATOR_URL}{action['url']}"
        method = action['method'].upper()

        request_map = {
            "POST": http_client.client.post,
            "GET": http_client.client.get,
            "PUT": http_client.client.put,
            "DELETE": http_client.client.delete,
        }

        request_func = request_map.get(method)
        if not request_func:
            return {"error": f"Unsupported method: {method}"}

        logging.info(f"Executing action: {method} {url} with payload {collected_data}")

        kwargs = {}
        if method in ["POST", "PUT"]:
            kwargs['json'] = collected_data
        else:
            kwargs['params'] = collected_data

        response = await request_func(url, **kwargs)
        response.raise_for_status()
        return response.json()


bot_service = BotService()