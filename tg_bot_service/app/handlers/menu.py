import logging
import re
from typing import Dict
from aiogram import Bot
from aiogram.types import Message, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder

from app.kafka.producer import kafka_producer
from app.services.storage_service import storage_service
from app.bot.states import ActionForm, WishlistAddManual
from app.bot.utils import load_schema, find_item_by_name, reset_to_main_menu
from app.bot.keyboards import build_menu_keyboard
from app.bot.constants import UPLOAD_MEDIA_BUTTON, VIEW_ALBUMS_BUTTON
from app.core.config import settings

logger = logging.getLogger(__name__)


async def menu_handler(message: Message, state: FSMContext) -> None:
    if not message.text or not message.from_user: return
    user_id = message.from_user.id
    logger.debug(f"Menu handler: '{message.text}' from {user_id}")

    if message.text == UPLOAD_MEDIA_BUTTON:
        from .media import start_media_upload_handler
        await start_media_upload_handler(message, state)
        return

    if message.text == VIEW_ALBUMS_BUTTON:
        from .albums import start_album_view_handler
        await start_album_view_handler(message, state)
        return

    url_match = re.search(r'https?://[^\s/$.?#].[^\s]*', message.text)
    if url_match:
        logger.info(f"User {user_id} sent text with URL: {message.text}")
        builder = InlineKeyboardBuilder()
        builder.button(text="🎁 Обычный (1 бронь)", callback_data=f"wishlist_add_url:single:{message.text}")
        builder.button(text="♾️ Общий (много броней)", callback_data=f"wishlist_add_url:infinite:{message.text}")
        builder.button(text="❌ Отмена", callback_data="wishlist_add_url:cancel")
        builder.adjust(2, 1)
        await message.answer(
            "Добавляем товар по ссылке. Этот товар 'общий' (его могут забронировать несколько) или 'обычный' (только один)?",
            reply_markup=builder.as_markup())
        return

    data = await state.get_data()
    current_node = data.get("current_node", await load_schema())
    selected = await find_item_by_name(message.text, current_node, user_id)

    if selected is None:
        is_admin_button = any(
            item.get("name") == message.text and item.get("admin_only") for item in current_node.get("items", []) if
            isinstance(item, dict))
        if is_admin_button:
            await message.answer("Только для админов.")
        else:
            await message.answer(f"Пункт '{message.text}' ты здесь не найдешь. Давай че-нить другое.")
        return

    if selected.get("type") == "menu":
        is_admin = user_id in settings.ADMIN_TELEGRAM_IDS
        sub_items = selected.get("items") or []
        sub_names = [i.get("name") for i in sub_items if
                     isinstance(i, Dict) and isinstance(i.get("name"), str) and (not i.get("admin_only") or is_admin)]
        if sub_names:
            await state.update_data(current_node=selected)
            kb = build_menu_keyboard(sub_names, add_start=True, add_back_to_welcome=False)
            await message.answer("Кликай!", reply_markup=kb)
        else:
            await message.answer("Ну и как ты сюда попал?..")
        return

    if selected.get("type") == "action":
        await start_form_action(selected, message, state)


async def start_form_action(action: dict, message: Message, state: FSMContext):
    payload_schema = action.get("payload", {})
    fields = list(payload_schema.keys())
    logger.info(f"Starting action: {action.get('name')} for {message.from_user.id}")

    kafka_topic = action.get("kafka_topic")

    if kafka_topic == "wishlist.item.add_manual_start":
        await state.set_state(WishlistAddManual.waiting_for_name)
        await state.update_data(manual_add_data={
            "telegram_id": message.from_user.id
        })
        await message.answer("Введи название товара:", reply_markup=build_menu_keyboard([], add_start=True))
        return

    if not fields:
        collected_data = {}
        if message.from_user:
            collected_data["telegram_id"] = message.from_user.id
            if action.get("kafka_topic") == "user.user.list_request":
                collected_data["admin_id"] = message.from_user.id

        await message.answer("Ля, погодь, я думаю...")

        try:
            if not kafka_topic: raise ValueError("No kafka_topic")

            await kafka_producer.send(kafka_topic, collected_data)
            logger.info(f"Action {kafka_topic} (no fields) sent")
        except Exception as e:
            logger.error(f"Kafka error: {e}", exc_info=True)
            await message.answer(f"Ошибка: {e}")
        finally:
            if action.get("unfinished"):
                logger.debug("Action unfinished, not resetting menu.")
                return
            await reset_to_main_menu(message, state, is_action_finish=True)
    else:
        await state.set_state(ActionForm.waiting_for_field)
        await state.update_data(action=action, fields=fields, current_field_index=0, collected_data={})
        field_name = fields[0]
        field_info = payload_schema[field_name]
        await message.answer(f"Введите '{field_info['description']}' ({field_info['type']}):",
                             reply_markup=build_menu_keyboard([], add_start=True))


async def process_action_field(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    action = data["action"]
    fields = data["fields"]
    current_field_idx = data["current_field_index"]
    collected_data = data["collected_data"]
    current_field_name = fields[current_field_idx]
    field_info = action["payload"][current_field_name]
    field_type = field_info.get("type")

    logger.debug(f"Processing field {current_field_name} ({field_type}) for {message.from_user.id}")
    input_value = None

    if field_type == "file":
        if not message.document:
            await message.answer("Кинь файлик")
            return
        doc = message.document
        await message.answer(f"Кроду '{doc.file_name}' на сервер...")
        file_info = await bot.get_file(doc.file_id)
        file_bytes = await bot.download_file(file_info.file_path)
        try:
            input_value = storage_service.upload_file(file_bytes.read(), doc.file_name, folder="tickets",
                                                      content_type=doc.mime_type or "application/pdf")
            await message.answer("Файлик успешно национализирован.")
            logger.info(f"Uploaded {input_value}")
        except Exception as e:
            logger.error(f"MinIO Error: {e}", exc_info=True)
            await message.answer("Плохой файл, у меня от него живот болит, чет не то.")
            return
    elif field_type == "integer":
        if not message.text or not message.text.isdigit():
            await message.answer("Нужно число (ID).")
            return
        input_value = int(message.text)
    else:
        if not message.text:
            await message.answer("Букавы пиши.")
            return
        input_value = message.text

    collected_data[current_field_name] = input_value
    next_field_idx = current_field_idx + 1

    if next_field_idx < len(fields):
        await state.update_data(current_field_index=next_field_idx, collected_data=collected_data)
        next_field_name = fields[next_field_idx]
        next_field_info = action["payload"][next_field_name]
        await message.answer(f"Введите '{next_field_info['description']}' ({next_field_info['type']}):")
    else:
        collected_data["telegram_id"] = message.from_user.id
        if action.get("kafka_topic") == "user.user.allow_request":
            collected_data["admin_id"] = message.from_user.id
        try:
            kafka_topic = action.get("kafka_topic")
            if not kafka_topic: raise ValueError("No kafka_topic")
            logger.info(f"Action {kafka_topic} ready: {collected_data}")
            await kafka_producer.send(kafka_topic, collected_data)
            await message.answer("Я подумаю над этим на досуге.")
        except Exception as e:
            logger.error(f"Kafka error: {e}", exc_info=True)
            await message.answer(f"Ошибка: {e}")
        finally:
            if action.get("unfinished"):
                logger.debug("Action unfinished")
                return
            await reset_to_main_menu(message, state, is_action_finish=True)