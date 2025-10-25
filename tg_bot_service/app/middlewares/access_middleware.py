import logging
from typing import Callable, Dict, Any, Awaitable, Set

from aiogram import BaseMiddleware
from aiogram.types import Message, Update

from app.core.config import settings

logger = logging.getLogger(__name__)

allowed_user_ids: Set[int] = set(settings.ADMIN_TELEGRAM_IDS) | set(settings.ALLOWED_TELEGRAM_IDS)
logger.info(f"Initialized allowed users from .env: {len(allowed_user_ids)} users.")


def update_allowed_users(user_id: int, allow: bool):
    global allowed_user_ids
    if allow:
        if user_id not in allowed_user_ids:
            allowed_user_ids.add(user_id)
            logger.info(f"User {user_id} added to allowed list (runtime). Current list: {allowed_user_ids}")
        else:
            logger.info(f"User {user_id} is already in allowed list.")
    else:
        is_admin = user_id in settings.ADMIN_TELEGRAM_IDS
        is_env_allowed = user_id in settings.ALLOWED_TELEGRAM_IDS

        if user_id in allowed_user_ids:
            if is_admin:
                logger.warning(f"Attempt to remove admin user {user_id} ignored.")
            elif is_env_allowed:
                logger.warning(f"Attempt to remove user {user_id} specified in ALLOWED_TELEGRAM_IDS ignored.")
            else:
                allowed_user_ids.remove(user_id)
                logger.info(f"User {user_id} removed from allowed list (runtime). Current list: {allowed_user_ids}")
        else:
            logger.warning(f"Attempt to remove non-listed user {user_id} ignored.")


class AccessMiddleware(BaseMiddleware):
    async def __call__(
            self,
            handler: Callable[[Update, Dict[str, Any]], Awaitable[Any]],
            event: Update,
            data: Dict[str, Any]
    ) -> Any:

        user = data.get('event_from_user')

        if not user:
            logger.debug(f"Passing update {event.update_id}: no user associated.")
            return await handler(event, data)

        user_id = user.id

        if user_id in allowed_user_ids:
            logger.debug(f"Access granted for {user_id}")
            return await handler(event, data)

        logger.warning(f"Access denied for {user_id} (Update ID: {event.update_id})")

        try:
            if event.message:
                await event.message.answer("Доступ запрещен. Обратитесь к администратору.")
            elif event.callback_query:
                await event.callback_query.answer("Доступ запрещен.", show_alert=True)
        except Exception as e:
            logger.error(f"Failed to send access denied reply for update {event.update_id}: {e}")

        return