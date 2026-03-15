import logging
import re
from typing import Dict, List, Optional
from datetime import datetime

from aiogram.types import Message
from aiogram.fsm.context import FSMContext

from app.core.config import settings
from app.core.http_client import http_client
from app.bot.keyboards import build_menu_keyboard
from app.bot.constants import (
    UPLOAD_MEDIA_BUTTON, VIEW_ALBUMS_BUTTON,
    ADMIN_ADD_USER_BUTTON, ADMIN_REMOVE_USER_BUTTON
)

logger = logging.getLogger(__name__)

def escape_markdown(text: str) -> str:
    if not isinstance(text, str): return ""
    escape_chars = r"_*[]()~`>#+-=|{}.!"
    pattern = f"([{re.escape(escape_chars)}])"
    return re.sub(pattern, r"\\\1", text)

def get_current_new_year() -> Optional[int]:
    now = datetime.now()
    if now.month == 1 and now.day < 15: return now.year
    if now.month == 12: return now.year + 1
    return None

def get_media_folder(album_id: str) -> str:
    return f"photos/{album_id}"

def get_schema_loader():
    schema_cache: Dict = {}

    async def load_schema(flag=0) -> Dict:
        if schema_cache and flag == 0: return schema_cache
        try:
            response = await http_client.client.get(f"{settings.ORCHESTRATOR_URL}/api/v1/menu")
            response.raise_for_status()
            schema_cache.update(response.json())
            logger.info("Схема меню успешно загружена/обновлена.")
            return schema_cache
        except Exception as exc:
            logger.exception("Не удалось загрузить schema.json: %s", exc)
            schema_cache.clear()
            return schema_cache

    return load_schema

load_schema = get_schema_loader()

async def get_root_items(user_id: int) -> List[str]:
    schema = await load_schema()
    items = schema.get("items", [])
    is_admin = user_id in settings.ADMIN_TELEGRAM_IDS

    item_names = [
        item.get("name") for item in items
        if isinstance(item, dict) and isinstance(item.get("name"), str)
           and (not item.get("admin_only") or is_admin)
    ]

    return item_names

async def find_item_by_name(target_name: str, node: Dict, user_id: int) -> Optional[Dict]:
    if not isinstance(node, dict): return None

    schema = await load_schema()
    if not schema:
        logger.warning("Schema is empty in find_item_by_name")
        return None

    children = node.get("items") or []
    is_admin = user_id in settings.ADMIN_TELEGRAM_IDS

    for child in children:
        if isinstance(child, dict) and child.get("name") == target_name:
            if child.get("admin_only", False) and not is_admin:
                continue
            return child

    if is_admin:
        if target_name == ADMIN_ADD_USER_BUTTON:
            allow_action = next((item for item in schema.get("items", []) if
                                 isinstance(item, dict) and item.get("kafka_topic") == "user.user.allow_request"), None)
            return allow_action
        if target_name == ADMIN_REMOVE_USER_BUTTON:
            list_action = next((item for item in schema.get("items", []) if
                                isinstance(item, dict) and item.get("kafka_topic") == "user.user.list_request"), None)
            return list_action
    return None

async def reset_to_main_menu(message: Message, state: FSMContext, is_action_finish: bool = False):
    await state.clear()
    logging.info("state_clear")
    user_id = message.from_user.id
    item_names = await get_root_items(user_id)
    item_names.extend([UPLOAD_MEDIA_BUTTON, VIEW_ALBUMS_BUTTON])
    kb = build_menu_keyboard(item_names, add_start=False, add_back_to_welcome=True)
    schema = await load_schema()
    await state.update_data(current_node=schema)
    text = "Кликай меню:"
    if is_action_finish:
        text = "Я всё. Чем ещё займёшь?"
    await message.answer(text, reply_markup=kb)
    logger.debug(f"Reset to main menu for {user_id}")