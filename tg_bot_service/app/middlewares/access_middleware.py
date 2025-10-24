import logging
from typing import Callable, Dict, Any, Awaitable, Set

from aiogram import BaseMiddleware
from aiogram.exceptions import CancelHandler
from aiogram.types import Message

from app.core.config import settings
from app.core.http_client import http_client

logger = logging.getLogger(__name__)

allowed_user_ids: Set[int] = set(settings.ADMIN_TELEGRAM_IDS)

async def fetch_allowed_users():
    try:
        response = await http_client.client.get(f"{settings.ORCHESTRATOR_URL}/api/v1/users/allowed")
        response.raise_for_status()
        user_ids = response.json()
        allowed_user_ids.clear()
        allowed_user_ids.update(settings.ADMIN_TELEGRAM_IDS)
        allowed_user_ids.update(user_ids)
        logger.info(f"Allowed users updated: {len(allowed_user_ids)} users.")
    except Exception as e:
        logger.error(f"Failed to fetch allowed users: {e}")

def update_allowed_users(user_id: int, allow: bool):
    if allow:
        allowed_user_ids.add(user_id)
        logger.info(f"User {user_id} added to allowed list (runtime).")
    else:
        if user_id in allowed_user_ids and user_id not in settings.ADMIN_TELEGRAM_IDS:
            allowed_user_ids.remove(user_id)
            logger.info(f"User {user_id} removed from allowed list (runtime).")
        else:
            logger.warning(f"Attempt to remove non-listed or admin user {user_id} ignored.")


class AccessMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
        event: Message,
        data: Dict[str, Any]
    ) -> Any:
        user_id = event.from_user.id
        if user_id in allowed_user_ids:
            logger.debug(f"Access granted for {user_id}")
            return await handler(event, data)

        logger.warning(f"Access denied for {user_id}")
        await event.answer("Доступ запрещен. Обратитесь к администратору.")
        raise CancelHandler()