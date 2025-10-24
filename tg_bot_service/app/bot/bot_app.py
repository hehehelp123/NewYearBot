import logging
import re
from typing import Dict, List, Optional
from datetime import datetime

from aiogram import F, Bot
from aiogram.types import (
    Message, ReplyKeyboardMarkup, KeyboardButton, Document, CallbackQuery,
    InlineKeyboardButton, URLInputFile
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder, InlineKeyboardMarkup
from aiogram.filters import StateFilter

from app.core.config import settings
from app.kafka.producer import kafka_producer
from app.services.storage_service import storage_service
from app.services.bot_service import bot_service
from app.core.http_client import http_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ActionForm(StatesGroup):
    waiting_for_field = State()


class WishlistBrowser(StatesGroup):
    browsing = State()


START_BUTTON = "🔄 Старт"

# ЗАМЕНИ ЭТОТ ID НА ТОТ, ЧТО ПОЛУЧИШЬ ОТ БОТА
WELCOME_IMAGE_FILE_ID = "PASTE_YOUR_FILE_ID_HERE"

WELCOME_TEXT = (
    "Добро пожаловать на нашу новогоднюю вечеринку! 🎄✨\n\n"
    "Этот бот — твой личный помощник во всем, что касается нашего праздника.\n\n"
    "Используй кнопки ниже, чтобы узнать пароль от WiFi, "
    "связаться с организаторами или перейти в главное меню, чтобы... "
    "ну, ты сам все увидишь! 😉\n\n"
    "С наступающим!"
)


def escape_markdown(text: str) -> str:
    if not isinstance(text, str):
        return ""
    escape_chars = r"[_*\[\]()~`>#\+\-=|{}.!]"
    return re.sub(f"({escape_chars})", r"\\\1", text)


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


async def get_root_items() -> List[str]:
    schema = await load_schema()
    items = schema.get("items", [])
    return [item.get("name") for item in items if isinstance(item, Dict) and isinstance(item.get("name"), str)]


def find_item_by_name(target_name: str, node: Dict) -> Optional[Dict]:
    if not isinstance(node, Dict): return None
    children = node.get("items") or []
    for child in children:
        if isinstance(child, Dict) and child.get("name") == target_name:
            return child
    return None


def build_menu_keyboard(item_names: List[str], add_start: bool = False) -> ReplyKeyboardMarkup:
    row, rows = [], []
    for i, name in enumerate(item_names, start=1):
        row.append(KeyboardButton(text=name))
        if i % 2 == 0:
            rows.append(row)
            row = []
    if row: rows.append(row)
    if add_start: rows.append([KeyboardButton(text=START_BUTTON)])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


async def start_button_handler(message: Message, state: FSMContext) -> None:
    logger.debug(f"Нажата кнопка 'Старт' пользователем {message.from_user.id}")
    if message.text == START_BUTTON:
        await state.clear()
        item_names = await get_root_items()
        kb = build_menu_keyboard(item_names)

        await state.update_data(current_node=await load_schema())
        await message.answer("Выберите пункт меню:", reply_markup=kb)
        return


async def welcome_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🔑 Пароль от WiFi", callback_data="info:wifi")
    builder.button(text="🆘 Связь с админами", callback_data="info:admins")
    builder.button(text="🚀 Поехали! (Главное меню)", callback_data="info:go_to_main_menu")
    builder.adjust(2, 1)
    return builder.as_markup()


# --- ВРЕМЕННЫЙ ХЭНДЛЕР: НАЧАЛО ---
# (Этот хэндлер нужно будет удалить после получения ID)
async def get_photo_id_handler(message: Message):
    if message.photo:
        file_id = message.photo[-1].file_id
        logger.info(f"ПОЛУЧЕН FILE_ID: {file_id}")
        await message.answer(f"Photo `file_id`:\n`{file_id}`", parse_mode="MarkdownV2")


# --- ВРЕМЕННЫЙ ХЭНДЛЕР: КОНЕЦ ---


async def start_handler(message: Message, state: FSMContext) -> None:
    await load_schema(flag=1)
    await state.clear()
    user = message.from_user

    logger.info(f"Запуск /start для пользователя {user.id} ({user.username})")

    if user:
        user_data = {"telegram_id": user.id, "username": user.username or user.full_name}
        try:
            await kafka_producer.send("user.user.create", user_data)
            await kafka_producer.send("wishlist.wishlist.create", user_data)
            logger.info(f"События 'create' для user и wishlist отправлены для {user.id}")
        except Exception as e:
            logger.error(f"Ошибка отправки Kafka-сообщений при /start для {user.id}: {e}")

    kb = await welcome_keyboard()

    try:
        await message.answer_photo(
            photo=WELCOME_IMAGE_FILE_ID,
            caption=WELCOME_TEXT,
            reply_markup=kb
        )
    except Exception as e:
        logger.error(f"Не удалось отправить фото по FILE_ID ({WELCOME_IMAGE_FILE_ID}): {e}. Попробуем отправить текст.")
        await message.answer(WELCOME_TEXT, reply_markup=kb)


async def show_main_menu_callback(query: CallbackQuery, state: FSMContext):
    await query.answer()

    item_names = await get_root_items()
    if item_names:
        await state.update_data(current_node=await load_schema())
        kb = build_menu_keyboard(item_names)
        await query.message.answer("Добро пожаловать! Выберите пункт меню:", reply_markup=kb)
    else:
        await query.message.answer("Схема меню пуста или не найдена.")


async def show_info_callback(query: CallbackQuery):
    action = query.data.split(":")[-1]

    if action == "wifi":
        logger.debug(f"Пользователь {query.from_user.id} запросил WiFi")
        await query.answer(f"Пароль от WiFi: {settings.WIFI_PASSWORD}", show_alert=True)

    elif action == "admins":
        logger.debug(f"Пользователь {query.from_user.id} запросил контакты админов")
        builder = InlineKeyboardBuilder()
        for name, user_id in settings.ADMINS_MAP.items():
            builder.button(text=name, url=f"tg://user?id={user_id}")
        builder.button(text="⬅️ Назад", callback_data="info:back_to_welcome")
        builder.adjust(1)

        await query.message.edit_caption(
            caption="Вот наши организаторы. По любым вопросам — сразу к ним!",
            reply_markup=builder.as_markup()
        )
        await query.answer()

    elif action == "back_to_welcome":
        logger.debug(f"Пользователь {query.from_user.id} вернулся в стартовое меню")
        kb = await welcome_keyboard()
        await query.message.edit_caption(
            caption=WELCOME_TEXT,
            reply_markup=kb
        )
        await query.answer()


async def menu_handler(message: Message, state: FSMContext) -> None:
    if not message.text or not message.from_user: return
    logger.debug(f"Меню-хэндлер: {message.text} от {message.from_user.id}")

    data = await state.get_data()
    current_node = data.get("current_node", await load_schema())
    selected = find_item_by_name(message.text, current_node)
    if selected is None:
        await message.answer(f"Пункт '{message.text}' не найден. Попробуйте снова.")
        return

    if selected.get("type") == "menu":
        sub_items = selected.get("items") or []
        sub_names = [i.get("name") for i in sub_items if isinstance(i, Dict) and isinstance(i.get("name"), str)]
        if sub_names:
            await state.update_data(current_node=selected)
            kb = build_menu_keyboard(sub_names, add_start=True)
            await message.answer("Выберите пункт меню:", reply_markup=kb)
        else:
            await message.answer("В этом меню нет пунктов.")
        return

    if selected.get("type") == "action":
        await start_form_action(selected, message, state)


async def start_form_action(action: dict, message: Message, state: FSMContext):
    payload_schema = action.get("payload", {})
    fields = list(payload_schema.keys())

    logger.info(f"Запуск action: {action.get('name')} для {message.from_user.id}")

    if not fields:
        collected_data = {}
        if message.from_user:
            collected_data["telegram_id"] = message.from_user.id

        await message.answer("Выполняю запрос...")

        try:
            kafka_topic = action.get("kafka_topic")
            if not kafka_topic:
                raise ValueError("В схеме не указан kafka_topic для этого действия")

            await kafka_producer.send(kafka_topic, collected_data)
            logger.info(f"Action {kafka_topic} (no fields) отправлен в Kafka")
        except Exception as e:
            logger.error(f"Ошибка отправки в Kafka: {e}", exc_info=True)
            await message.answer(f"Ошибка: {e}")
        finally:
            await reset_to_main_menu(message, state)
    else:
        await state.set_state(ActionForm.waiting_for_field)
        await state.update_data(action=action, fields=fields, current_field_index=0, collected_data={})
        field_name = fields[0]
        field_info = payload_schema[field_name]
        await message.answer(
            f"Введите '{field_info['description']}' ({field_info['type']}):",
            reply_markup=build_menu_keyboard([], add_start=True)
        )


async def process_action_field(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    action = data["action"]
    fields = data["fields"]
    current_field_idx = data["current_field_index"]
    collected_data = data["collected_data"]
    current_field_name = fields[current_field_idx]
    field_info = action["payload"][current_field_name]
    field_type = field_info.get("type")

    logger.debug(f"Обработка поля {current_field_name} (тип: {field_type}) для {message.from_user.id}")

    if field_type == "file":
        if not message.document:
            await message.answer("Пожалуйста, прикрепите файл.")
            return
        doc = message.document
        await message.answer(f"Загружаю файл '{doc.file_name}' на сервер...")
        file_info = await bot.get_file(doc.file_id)
        file_bytes = await bot.download_file(file_info.file_path)

        try:
            object_name = storage_service.upload_file(file_bytes.read(), doc.file_name)
            collected_data[current_field_name] = object_name
            await message.answer("Файл успешно загружен.")
            logger.info(f"Файл {object_name} загружен в MinIO")
        except Exception as e:
            logger.error(f"Ошибка загрузки файла в MinIO: {e}", exc_info=True)
            await message.answer("Произошла ошибка при загрузке файла. Попробуйте еще раз.")
            return

    else:
        if not message.text:
            await message.answer("Пожалуйста, введите текст.")
            return
        collected_data[current_field_name] = message.text

    next_field_idx = current_field_idx + 1
    if next_field_idx < len(fields):
        await state.update_data(current_field_index=next_field_idx, collected_data=collected_data)
        next_field_name = fields[next_field_idx]
        next_field_info = action["payload"][next_field_name]
        await message.answer(f"Введите '{next_field_info['description']}' ({next_field_info['type']}):")
    else:
        if message.from_user: collected_data["telegram_id"] = message.from_user.id

        try:
            kafka_topic = action.get("kafka_topic")
            if not kafka_topic:
                raise ValueError("В схеме не указан kafka_topic для этого действия")

            logger.info(f"Action {kafka_topic} (с полями) готов к отправке в Kafka")
            await kafka_producer.send(kafka_topic, collected_data)
            await message.answer("Ваш запрос принят в обработку!")
        except Exception as e:
            logger.error(f"Ошибка отправки в Kafka: {e}", exc_info=True)
            await message.answer(f"Ошибка: {e}")
        finally:
            if (action.get("unfinished") == True):
                logger.debug("Action помечен как 'unfinished', не сбрасываем меню.")
                return
            await reset_to_main_menu(message, state)


async def callback_query_handler(query: CallbackQuery, bot: Bot, state: FSMContext):
    logger.debug(f"Callback Query Handler: {query.data} от {query.from_user.id}")

    action, value = query.data.split(":", 1)
    ticket_id = int(value)
    user_id = query.from_user.id

    if action == "download_ticket":
        await query.answer("Запрос на скачивание отправлен...")
        command_path = "ticket_download_request"
        action_schema = {"method": "POST", "url": f"/api/v1/commands/{command_path}"}
        payload = {"telegram_id": user_id, "ticket_id": ticket_id}
        await bot_service.execute_action(action_schema, payload)
        logger.info(f"Запрос 'download_ticket' (ticket_id: {ticket_id}) отправлен.")

    elif action == "delete_ticket":
        builder = InlineKeyboardBuilder()
        builder.button(text="Да, удалить", callback_data=f"confirm_delete:{ticket_id}")
        builder.button(text="Нет, отмена", callback_data=f"cancel_delete:{ticket_id}")
        await query.message.edit_text(
            f"Вы уверены, что хотите удалить билет **{query.message.text.splitlines()[0]}**?",
            reply_markup=builder.as_markup(),
            parse_mode="MarkdownV2"
        )
        await query.answer()

    elif action == "confirm_delete":
        await query.answer("Запрос на удаление отправлен...")
        command_path = "ticket_delete_request"
        action_schema = {"method": "POST", "url": f"/api/v1/commands/{command_path}"}
        payload = {"telegram_id": user_id, "ticket_id": ticket_id}
        await bot_service.execute_action(action_schema, payload)
        await query.message.delete()
        logger.info(f"Запрос 'confirm_delete' (ticket_id: {ticket_id}) отправлен.")

    elif action == "cancel_delete":
        await query.message.edit_text(query.message.text, entities=query.message.entities, reply_markup=None)
        await query.answer("Удаление отменено.")


async def reset_to_main_menu(message: Message, state: FSMContext):
    await state.clear()
    item_names = await get_root_items()
    kb = build_menu_keyboard(item_names)

    await state.update_data(current_node=await load_schema())
    await message.answer("Выберите пункт меню:", reply_markup=kb)
    logger.debug(f"Сброс в главное меню для {message.from_user.id}")


async def build_wishlist_page(state: FSMContext, viewer_user_id: int) -> (str, InlineKeyboardMarkup):
    data = await state.get_data()
    items = data.get("items", [])
    current_index = data.get("current_index", 0)
    owner_user_id = data.get("owner_user_id", 0)

    logger.debug(f"Построение страницы вишлиста: index {current_index} для viewer {viewer_user_id}")

    if not items or current_index >= len(items):
        logger.warning("Попытка построить страницу вишлиста, но он пуст.")
        return "Wishlist is empty\\.", None

    item = items[current_index]
    item_price = escape_markdown(item.get("cost", "N/A"))
    item_delivery = escape_markdown(item.get("delivery_date", "N/A"))
    item_name = escape_markdown(item.get("name", "N/A"))
    item_url = item.get("item_url", "N/A")

    text = f"*Товар {current_index + 1} из {len(items)}*\n\n"
    text += f"*Название:* {item_name}\n"
    if item_url:
        text += f"*URL:* [Link]({item_url})\n"

    booking_info = item.get("booking")
    is_owner = (owner_user_id == viewer_user_id)
    text += f"*Цена:* {item_price}\n"
    text += f"*Доставка:* {item_delivery}\n"
    builder = InlineKeyboardBuilder()

    nav_buttons = []
    if current_index > 0:
        nav_buttons.append(InlineKeyboardButton(text="⬅️ Назад", callback_data="wishlist_prev"))

    nav_buttons.append(InlineKeyboardButton(text="❌ Закрыть", callback_data="wishlist_close"))

    if current_index < len(items) - 1:
        nav_buttons.append(InlineKeyboardButton(text="Вперед ➡️", callback_data="wishlist_next"))
    builder.row(*nav_buttons)

    item_id = item.get('item_id', 'unknown')

    if booking_info:
        booker_id = booking_info.get("booked_by_user_id")
        if booker_id == viewer_user_id:
            text += f"\n*Статус:* 🎁 Вы забронировали этот товар\\!\n"
            builder.button(text="🎁 Снять бронь", callback_data=f"wishlist_unbook:{item_id}")
        else:
            text += f"\n*Статус:* ⛔️ Забронирован пользователем {booker_id}\n"
            builder.button(text="⛔️ Забронирован", callback_data="wishlist_noop")
    elif is_owner:
        text += f"\n*Статус:* ✅ Ваш товар\\. Доступен для бронирования\n"
        builder.button(text="🗑️ Удалить", callback_data=f"wishlist_delete:{item_id}")
    else:
        text += f"\n*Статус:* ✅ Доступен для бронирования\n"
        builder.button(text="🎁 Забронировать", callback_data=f"wishlist_book:{item_id}")

    return text, builder.as_markup()


async def wishlist_navigation_handler(query: CallbackQuery, state: FSMContext, bot: Bot):
    action, *value = query.data.split(":")
    data = await state.get_data()
    items = data.get("items", [])
    current_index = data.get("current_index", 0)

    logger.debug(f"Навигация по вишлисту: {query.data} от {query.from_user.id}")

    new_index = current_index

    if action == "wishlist_next":
        if current_index < len(items) - 1:
            new_index = current_index + 1
        await state.update_data(current_index=new_index)

    elif action == "wishlist_prev":
        if current_index > 0:
            new_index = current_index - 1
        await state.update_data(current_index=new_index)

    elif action == "wishlist_close":
        await query.message.delete()
        await reset_to_main_menu(query.message, state)
        await query.answer()
        return

    elif action == "wishlist_book":
        item_id = int(value[0])
        await kafka_producer.send(
            "wishlist.item.book",
            {"item_id": item_id, "booker_user_id": query.from_user.id}
        )
        logger.info(f"Пользователь {query.from_user.id} забронировал item {item_id}")

        if items and 0 <= current_index < len(items):
            items[current_index]['booking'] = {
                'booked_by_user_id': query.from_user.id,
                'booked_at': datetime.utcnow().isoformat()
            }
            await state.update_data(items=items)

        text, markup = await build_wishlist_page(state, query.from_user.id)
        await query.message.edit_text(text, reply_markup=markup, parse_mode="MarkdownV2")

        await query.answer("✅ Забронировано!")
        return

    elif action == "wishlist_unbook":
        item_id = int(value[0])
        await kafka_producer.send(
            "wishlist.item.unbook",
            {"item_id": item_id, "unbooker_user_id": query.from_user.id}
        )
        logger.info(f"Пользователь {query.from_user.id} снял бронь с item {item_id}")

        if items and 0 <= current_index < len(items):
            items[current_index]['booking'] = None
            await state.update_data(items=items)

        text, markup = await build_wishlist_page(state, query.from_user.id)
        await query.message.edit_text(text, reply_markup=markup, parse_mode="MarkdownV2")

        await query.answer("✅ Бронь снята!")
        return

    elif action == "wishlist_delete":
        item_id = int(value[0])
        await kafka_producer.send(
            "wishlist.item.delete",
            {"item_id": item_id, "deleter_user_id": query.from_user.id}
        )
        logger.info(f"Пользователь {query.from_user.id} удалил item {item_id}")

        new_items = [item for item in items if item['item_id'] != item_id]

        if not new_items:
            await state.update_data(items=[], current_index=0)
            await query.message.edit_text("✅ Товар удален. Вишлист теперь пуст.", reply_markup=None)
            await query.answer("Товар удален. Вишлист пуст.", show_alert=True)
            return

        new_index = current_index
        if new_index >= len(new_items):
            new_index = max(0, len(new_items) - 1)

        await state.update_data(items=new_items, current_index=new_index)

        text, markup = await build_wishlist_page(state, query.from_user.id)
        await query.message.edit_text(text, reply_markup=markup, parse_mode="MarkdownV2")

        await query.answer("✅ Товар удален!")
        return

    elif action == "wishlist_noop":
        await query.answer()
        return

    try:
        text, markup = await build_wishlist_page(state, query.from_user.id)
        await query.message.edit_text(text, reply_markup=markup, parse_mode="MarkdownV2")
    except Exception as e:
        logger.warning(f"Error updating wishlist page: {e}")

    await query.answer()