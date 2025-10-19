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
                return HealthCheckResponse(status="error", message="Invalid Telegram API response")
            return HealthCheckResponse(
                status="healthy",
                message="Bot is operational",
                bot_id=data["result"].get("id"),
                username=data["result"].get("username")
            )
        except Exception as e:
            return HealthCheckResponse(status="error", message=f"Health check failed: {str(e)}")

    async def execute_action(self, action: dict, collected_data: dict) -> dict | bytes:
        url = f"{settings.ORCHESTRATOR_URL}{action['url']}"
        method = action['method'].upper()

        download_file = collected_data.pop('__download_file__', False)

        request_func = getattr(http_client.client, method.lower(), None)
        if not request_func:
            return {"error": f"Unsupported method: {method}"}

        kwargs = {}
        files_to_send = collected_data.pop('__files__', None)

        if files_to_send:
            kwargs['data'] = collected_data
            kwargs['files'] = files_to_send
        elif method in ["POST", "PUT"]:
            kwargs['json'] = collected_data
        else:
            kwargs['params'] = collected_data

        logging.info(f"Sending request to Orchestrator. Method: {method}, URL: {url}, Payload: {kwargs}")

        response = await request_func(url, **kwargs)
        response.raise_for_status()

        if download_file:
            return response.content
        return response.json()


bot_service = BotService()