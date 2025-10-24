import logging
from typing import Callable, Dict, Any, Awaitable, Set
from aiogram import BaseMiddleware
from aiogram.types import Update, User, Message
from aiogram.dispatcher.flags import get_flag
from aiogram.dispatcher.handler import CancelHandler, current_handler

from app.core.config import settings

logger = logging.getLogger(__name__)

allowed_user_ids: Set[int] = set(settings.ADMIN_TELEGRAM_IDS)
logger.info(f"AccessMiddleware initialized with admin IDs: {allowed_user_ids}")

def update_allowed_users(user_id: int, allow: bool):
    """Обновление set из Kafka consumer'а."""
    if allow:
        if user_id not in allowed_user_ids:
            allowed_user_ids.add(user_id)
            logger.info(f"User {user_id} added to allowed list.")
    else:
        if user_id in allowed_user_ids and user_id not in settings.ADMIN_TELEGRAM_IDS:
            allowed_user_ids.remove(user_id)
            logger.info(f"User {user_id} removed from allowed list.")
        elif user_id in settings.ADMIN_TELEGRAM_IDS:
             logger.warning(f"Attempted to remove admin {user_id} from allowed list. Action denied.")

class AccessMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[Update, Dict[str, Any]], Awaitable[Any]],
        event: Update,
        data: Dict[str, Any]
    ) -> Any:

        user: User | None = data.get("event_from_user")

        if not user:
            return await handler(event, data)

        if isinstance(event, Message) and event.text == "/start":
            logger.debug(f"Allowing /start for user {user.id}.")
            return await handler(event, data)

        if user.id not in allowed_user_ids:
            logger.warning(f"Access denied for user {user.id} (@{user.username}). Not in allowed list: {allowed_user_ids}")
            raise CancelHandler()

        logger.debug(f"Access granted for user {user.id}.")
        return await handler(event, data)