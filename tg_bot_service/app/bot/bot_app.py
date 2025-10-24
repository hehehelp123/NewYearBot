import logging
import re
import io
from typing import Dict, List, Optional
from datetime import datetime

from aiogram import F, Bot
from aiogram.types import (
    Message, ReplyKeyboardMarkup, KeyboardButton, Document, CallbackQuery,
    InlineKeyboardButton, URLInputFile, PhotoSize, Video, InputMediaPhoto, InputMediaVideo,
    BufferedInputFile
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder, InlineKeyboardMarkup
from aiogram.filters import StateFilter
from aiogram.exceptions import TelegramBadRequest

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

class MediaUpload(StatesGroup):
    waiting_for_year = State()
    uploading = State()

class AlbumBrowser(StatesGroup):
    choosing_year = State()
    browsing = State()

START_BUTTON = "🔄 Куда я жмав"
BACK_TO_WELCOME_BUTTON = "⬅️ Ещё разок приветствие"
UPLOAD_MEDIA_BUTTON = "📸 Загрузка нюдсов"
VIEW_ALBUMS_BUTTON = "🖼️ Смотреть кто что загрузил"
STOP_UPLOAD_BUTTON = "✅ Все чё мог то загрузил"

WELCOME_IMAGE_FILE_ID = "AgACAgIAAxkBAAIF1Gj74baO7XmV0gE64s7Acb28_VvNAALA9zEbLIbhS-7FY2lmbDJ-AQADAgADeAADNgQ"

WELCOME_TEXT = (
    "Доброго утра тебя, товарищ, и с наступающим (Новый год наступает тогда, когда ему хочется, а не по календарю) Новым годом! 🎄✨\n\n"
    "Этот бот создан упростить тебе жизнь, если мы с Максом не совсем долбоящеры, или сделать ее чуточку смешнее в противном случае.\n\n"
    "Кликай все, что кликается, по идее работает все, а если не работает то анлак"
    "Короче, бля, удачи 😉\n\n"
    "С наступающим!"
)

def escape_markdown(text: str) -> str:
    if not isinstance(text, str):
        return ""
    escape_chars = r"[_*\[\]()~`>#\+\-=|{}.!]"
    return re.sub(f"({escape_chars})", r"\\\1", text)

def get_current_new_year() -> Optional[int]:
    now = datetime.now()
    if now.month == 1 and now.day < 15:
        return now.year
    if now.month == 12:
        return now.year + 1
    return None

def get_media_folder(year: int) -> str:
    return f"photos/{year}"

def get_schema_loader():
    schema_cache: Dict = {}

    async def load_schema(flag = 0) -> Dict:
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


def build_menu_keyboard(item_names: List[str], add_start: bool = False, add_back_to_welcome: bool = False) -> ReplyKeyboardMarkup:
    rows = []
    row = []
    all_items = list(item_names)

    if add_start:
        all_items.append(START_BUTTON)
    if add_back_to_welcome:
        all_items.append(BACK_TO_WELCOME_BUTTON)

    for i, name in enumerate(all_items):
        row.append(KeyboardButton(text=name))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


async def start_button_handler(message: Message, state: FSMContext) -> None:
    logger.debug(f"Нажата кнопка 'Старт' пользователем {message.from_user.id}")
    if message.text == START_BUTTON:
        await state.clear()
        item_names = await get_root_items()
        item_names.extend([UPLOAD_MEDIA_BUTTON, VIEW_ALBUMS_BUTTON])

        kb = build_menu_keyboard(item_names, add_start=False, add_back_to_welcome=True)

        await state.update_data(current_node=await load_schema())
        await message.answer("Ну кликни шо-нить:", reply_markup=kb)
        return

async def welcome_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🔑 Хочу WiFi", callback_data="info:wifi")
    builder.button(text="🆘 Кто тут у нас?", callback_data="info:admins")
    builder.button(text="🚀 Ну давай не томи", callback_data="info:go_to_main_menu")
    builder.adjust(2, 1)
    return builder.as_markup()

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

    if WELCOME_IMAGE_FILE_ID == "PASTE_YOUR_FILE_ID_HERE":
        logger.warning("WELCOME_IMAGE_FILE_ID не установлен! Отправка фото будет пропущена.")
        await message.answer(WELCOME_TEXT, reply_markup=kb)
        return

    try:
        await message.answer_photo(
            photo=WELCOME_IMAGE_FILE_ID,
            caption=WELCOME_TEXT,
            reply_markup=kb
        )
    except Exception as e:
        logger.error(f"Не удалось отправить фото по FILE_ID ({WELCOME_IMAGE_FILE_ID}): {e}. Попробуем отправить текст.")
        await message.answer(WELCOME_TEXT, reply_markup=kb)


async def back_to_welcome_handler(message: Message, state: FSMContext):
    logger.debug(f"Возврат к приветствию для {message.from_user.id}")
    await start_handler(message, state)


async def show_main_menu_callback(query: CallbackQuery, state: FSMContext):
    await query.answer()

    item_names = await get_root_items()
    item_names.extend([UPLOAD_MEDIA_BUTTON, VIEW_ALBUMS_BUTTON])

    if item_names:
        await state.update_data(current_node=await load_schema())
        kb = build_menu_keyboard(item_names, add_start=False, add_back_to_welcome=True)
        await query.message.answer("Доброе утро, мопсы! Выберите пункт меню (или просто посмотрите какие кайфовые тут смайлики, долго выбирал):", reply_markup=kb)
    else:
        await query.message.answer("Схема меню пуста или не найдена.")

async def show_info_callback(query: CallbackQuery):
    action = query.data.split(":")[-1]

    if action == "wifi":
        logger.debug(f"Пользователь {query.from_user.id} запросил WiFi")
        password = escape_markdown(settings.WIFI_PASSWORD)
        await query.message.answer(
            f"Пароль от WiFi:\n\n`{password}`",
            parse_mode="MarkdownV2"
        )
        await query.answer()

    elif action == "admins":
        logger.debug(f"Пользователь {query.from_user.id} запросил контакты админов")
        builder = InlineKeyboardBuilder()
        admin_map = settings.ADMINS_MAP

        if len(admin_map) == 1 and 1 in admin_map.values():
            await query.answer("Ошибка: ADMIN_TELEGRAM_IDS не настроены в .env файле!", show_alert=True)
            return

        for name, user_id in admin_map.items():
            builder.button(text=name, url=f"tg://user?id={user_id}")
        builder.button(text="⬅️ Куда я жмав...", callback_data="info:back_to_welcome")
        builder.adjust(1)

        await query.message.edit_caption(
            caption="Жабы со стажем, все вопросы к жабам (за эксзистенциальные будете наказаны).",
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
        await message.answer(f"Пункт '{message.text}' ты здесь не найдешь. Давай че-нить другое.")
        return

    if selected.get("type") == "menu":
        sub_items = selected.get("items") or []
        sub_names = [i.get("name") for i in sub_items if isinstance(i, Dict) and isinstance(i.get("name"), str)]
        if sub_names:
            await state.update_data(current_node=selected)
            kb = build_menu_keyboard(sub_names, add_start=True, add_back_to_welcome=True)
            await message.answer("Кликай!", reply_markup=kb)
        else:
            await message.answer("Ну и как ты сюда попал?..")
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

        await message.answer("Ля, погодь, я думаю...")

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
            await reset_to_main_menu(message, state, is_action_finish=True)
    else:
        await state.set_state(ActionForm.waiting_for_field)
        await state.update_data(action=action, fields=fields, current_field_index=0, collected_data={})
        field_name = fields[0]
        field_info = payload_schema[field_name]
        await message.answer(
            f"Введите '{field_info['description']}' ({field_info['type']}):",
            reply_markup=build_menu_keyboard([], add_start=True, add_back_to_welcome=True)
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
            await message.answer("Кинь файлик")
            return
        doc = message.document
        await message.answer(f"Кроду '{doc.file_name}' на сервер...")
        file_info = await bot.get_file(doc.file_id)
        file_bytes = await bot.download_file(file_info.file_path)

        try:
            object_name = storage_service.upload_file(
                file_bytes.read(),
                doc.file_name,
                folder="tickets",
                content_type=doc.mime_type or "application/pdf"
            )
            collected_data[current_field_name] = object_name
            await message.answer("Файлик успешно национализирован.")
            logger.info(f"Файл {object_name} загружен в MinIO")
        except Exception as e:
            logger.error(f"Ошибка загрузки файла в MinIO: {e}", exc_info=True)
            await message.answer("Плохой файл, у меня от него живот болит, чет не то.")
            return

    else:
        if not message.text:
            await message.answer("Букавы пиши.")
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
            await message.answer("Я подумаю над этим на досуге.")
        except Exception as e:
            logger.error(f"Ошибка отправки в Kafka: {e}", exc_info=True)
            await message.answer(f"Ошибка: {e}")
        finally:
            if (action.get("unfinished") == True):
                logger.debug("Action помечен как 'unfinished', не сбрасываем меню.")
                return
            await reset_to_main_menu(message, state, is_action_finish=True)


async def callback_query_handler(query: CallbackQuery, bot: Bot, state: FSMContext):

    logger.debug(f"Callback Query Handler: {query.data} от {query.from_user.id}")

    action, value = query.data.split(":", 1)
    ticket_id = int(value)
    user_id = query.from_user.id

    if action == "download_ticket":
        await query.answer("Ща поищу...")
        command_path = "ticket_download_request"
        action_schema = {"method": "POST", "url": f"/api/v1/commands/{command_path}"}
        payload = {"telegram_id": user_id, "ticket_id": ticket_id}
        await bot_service.execute_action(action_schema, payload)
        logger.info(f"Запрос 'download_ticket' (ticket_id: {ticket_id}) отправлен.")

    elif action == "delete_ticket":
        builder = InlineKeyboardBuilder()
        builder.button(text="Да, нахер билеты", callback_data=f"confirm_delete:{ticket_id}")
        builder.button(text="Не, погодь", callback_data=f"cancel_delete:{ticket_id}")
        await query.message.edit_text(
            f"Ты че, решил не приезжать с **{query.message.text.splitlines()[0]}**?",
            reply_markup=builder.as_markup(),
            parse_mode="MarkdownV2"
        )
        await query.answer()

    elif action == "confirm_delete":
        await query.answer("Очистка!..")
        command_path = "ticket_delete_request"
        action_schema = {"method": "POST", "url": f"/api/v1/commands/{command_path}"}
        payload = {"telegram_id": user_id, "ticket_id": ticket_id}
        await bot_service.execute_action(action_schema, payload)
        await query.message.delete()
        logger.info(f"Запрос 'confirm_delete' (ticket_id: {ticket_id}) отправлен.")

    elif action == "cancel_delete":
        await query.message.edit_text(query.message.text, entities=query.message.entities, reply_markup=None)
        await query.answer("Вернул как было!")


async def reset_to_main_menu(message: Message, state: FSMContext, is_action_finish: bool = False):
    await state.clear()
    item_names = await get_root_items()
    item_names.extend([UPLOAD_MEDIA_BUTTON, VIEW_ALBUMS_BUTTON])

    kb = build_menu_keyboard(item_names, add_start=False, add_back_to_welcome=True)

    await state.update_data(current_node=await load_schema())

    text = "Кликай меню:"
    if is_action_finish:
        text = "Я всё. Чем ещё займёшь?"

    await message.answer(text, reply_markup=kb)
    logger.debug(f"Сброс в главное меню для {message.from_user.id}")


async def start_media_upload_handler(message: Message, state: FSMContext):
    logger.info(f"Пользователь {message.from_user.id} начал загрузку медиа.")

    current_year = get_current_new_year()
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=STOP_UPLOAD_BUTTON)]],
        resize_keyboard=True
    )

    if current_year:
        await state.set_state(MediaUpload.uploading)
        await state.update_data(year=current_year)
        await message.answer(
            f"Режим загрузки фото/видео для Нового Года **{current_year}** включен.\n"
            "Отправляйте нюдсы (по одному или альбомом) (только если ты не Макс боже умоляю).\n"
            "Когда закончите, нажмите кнопку внизу.",
            reply_markup=kb,
            parse_mode="Markdown"
        )
    else:
        await state.set_state(MediaUpload.waiting_for_year)
        await message.answer(
            "Сейчас не 'новогодний сезон' если верить календарю вместо сердца.\n"
            "**Пожалуйста, введите год**, для которого вы хотите загрузить фото/видео (например, 2024).**",
            reply_markup=build_menu_keyboard([], add_back_to_welcome=True),
            parse_mode="Markdown"
        )

async def process_media_year_handler(message: Message, state: FSMContext):
    if not message.text or not message.text.isdigit():
        await message.answer("Пожалуйста, числом блять (например, 2024).")
        return

    year = int(message.text)
    if not (2020 < year < 2030):
        await message.answer("Пожалуйста, нормальное число, Макс (с 2021 по 2029).")
        return

    logger.info(f"Пользователь {message.from_user.id} выбрал {year} год для загрузки.")
    await state.set_state(MediaUpload.uploading)
    await state.update_data(year=year)

    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=STOP_UPLOAD_BUTTON)]],
        resize_keyboard=True
    )
    await message.answer(
        f"Режим загрузки фото/видео для **{year}** включен.\n"
        "Отправляйте нюдсы. Когда надоест, так и скажите.",
        reply_markup=kb,
        parse_mode="Markdown"
    )

async def stop_media_upload_handler(message: Message, state: FSMContext):
    logger.info(f"Пользователь {message.from_user.id} закончил загрузку медиа.")
    await state.clear()
    await message.answer("Неплохая работа.")
    await reset_to_main_menu(message, state)


async def media_upload_handler(message: Message, bot: Bot, state: FSMContext):
    if not (message.photo or message.video):
        await message.answer("Ну-ка, ну-ка, что тут у нас.")
        return

    data = await state.get_data()
    year = data.get("year")
    if not year:
        logger.warning(f"Нет года в FSM для {message.from_user.id}. Сбрасываем.")
        await stop_media_upload_handler(message, state)
        return

    file_id = ""
    file_unique_id = ""
    original_filename = ""
    content_type = ""

    if message.photo:
        media = message.photo[-1]
        file_id = media.file_id
        file_unique_id = media.file_unique_id
        content_type = "image/jpeg"
        original_filename = f"{message.from_user.id}_{file_unique_id}.jpg"
    elif message.video:
        media = message.video
        file_id = media.file_id
        file_unique_id = media.file_unique_id
        content_type = media.mime_type or "video/mp4"
        original_filename = f"{message.from_user.id}_{file_unique_id}.{content_type.split('/')[-1]}"


    logger.debug(f"Получено медиа {file_unique_id} от {message.from_user.id}")
    await message.answer(f"Украдываю (ID: ...{file_unique_id[-6:]})...")

    try:
        file_info = await bot.get_file(file_id)
        file_bytes_io = await bot.download_file(file_info.file_path)

        folder = get_media_folder(year)

        object_name = storage_service.upload_file(
            file_bytes_io.read(),
            original_filename,
            folder=folder,
            content_type=content_type
        )
        logger.info(f"Медиа {object_name} успешно загружено в {folder}")
        await message.answer(f"✅ Файл ...{file_unique_id[-6:]} национализирован!")

    except Exception as e:
        logger.error(f"Ошибка загрузки медиа {file_unique_id} в MinIO: {e}", exc_info=True)
        await message.answer(f"❌ Што ты наделал ...{file_unique_id[-6:]}.")


async def start_album_view_handler(message: Message, state: FSMContext):
    logger.debug(f"Пользователь {message.from_user.id} запросил просмотр альбомов.")
    await state.clear()

    try:
        response = await http_client.client.get(f"{settings.ORCHESTRATOR_URL}/api/v1/albums/years")
        response.raise_for_status()
        years = response.json()

        if not years:
            await message.answer("Пока нет ни одного загруженного альбома (базе пиздец):(.")
            return

        await state.set_state(AlbumBrowser.choosing_year)
        builder = InlineKeyboardBuilder()
        for year in years:
            builder.button(text=f"Альбом {year}", callback_data=f"album_year:{year}")
        builder.button(text="❌ Не, нафиг", callback_data="album_close")
        builder.adjust(2)

        await message.answer("Выберите альбом для просмотра:", reply_markup=builder.as_markup())

    except Exception as e:
        logger.error(f"Не удалось получить список годов альбомов: {e}")
        await message.answer("Не удалось загрузить список альбомов. Попробуйте написать Мише.")

async def build_album_page(state: FSMContext, bot: Bot, chat_id: int):
    data = await state.get_data()
    year = data.get("year")
    page = data.get("page", 1)
    message_id = data.get("message_id")

    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="Выбирай чо хошь", callback_data="album_menu"), InlineKeyboardButton(text="❌ Не-не-не", callback_data="album_close"))
    error_markup = builder.as_markup()

    try:
        response = await http_client.client.get(
            f"{settings.ORCHESTRATOR_URL}/api/v1/albums/{year}",
            params={"page": page, "page_size": 1}
        )
        response.raise_for_status()
        album_data = response.json()

        total_items = album_data.get("total_items", 0)
        current_page = album_data.get("page", 1)
        total_pages = album_data.get("total_pages", 0)
        items = album_data.get("items", [])

        if not items:
            await state.update_data(page=0)
            return "В этом альбоме могла бы быть ваша реклама.", error_markup, None, 0

        item = items[0]
        object_name = item.get("object_name")
        media_type = item.get("type", "photo")

        media_response = await http_client.client.get(f"{settings.ORCHESTRATOR_URL}/api/v1/albums/media/{object_name}")
        media_response.raise_for_status()
        media_bytes = media_response.content
        filename = object_name.split('/')[-1]

        caption = f"Альбом {year} | Файл {current_page} из {total_pages}"

        builder = InlineKeyboardBuilder()
        nav_buttons = []
        if current_page > 1:
            nav_buttons.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"album_page:{current_page - 1}"))

        nav_buttons.append(InlineKeyboardButton(text="🎲 Поиграем?~", callback_data="album_random"))

        if current_page < total_pages:
            nav_buttons.append(InlineKeyboardButton(text="Вперед ➡️", callback_data=f"album_page:{current_page + 1}"))

        builder.row(*nav_buttons)
        builder.row(InlineKeyboardButton(text="Меню годов", callback_data="album_menu"), InlineKeyboardButton(text="❌ Закрыть", callback_data="album_close"))

        input_file = BufferedInputFile(media_bytes, filename=filename)
        media_input = InputMediaPhoto(media=input_file) if media_type == "photo" else InputMediaVideo(media=input_file)

        return caption, builder.as_markup(), media_input, current_page

    except Exception as e:
        logger.error(f"Не удалось построить страницу альбома: {e}")
        return "Ошибка загрузки альбома.", error_markup, None, page


async def album_navigation_handler(query: CallbackQuery, bot: Bot, state: FSMContext):
    await query.answer()
    action, *value = query.data.split(":")

    if action == "album_close":
        await query.message.delete()
        await state.clear()
        return

    if action == "album_menu":
        await query.message.delete()
        await start_album_view_handler(query.message, state)
        return

    await state.set_state(AlbumBrowser.browsing)
    await state.update_data(message_id=query.message.message_id)

    if action == "album_year":
        year = int(value[0])
        await state.update_data(year=year, page=1)

    elif action == "album_page":
        page = int(value[0])
        await state.update_data(page=page)

    elif action == "album_random":
        data = await state.get_data()
        year = data.get("year")
        try:
            response = await http_client.client.get(f"{settings.ORCHESTRATOR_URL}/api/v1/albums/{year}/random")
            response.raise_for_status()
            random_data = response.json()
            new_page = random_data.get("page")
            if new_page:
                await state.update_data(page=new_page)
        except Exception as e:
            logger.error(f"Ошибка получения случайного медиа: {e}")
            await query.message.answer("Не удалось загрузить случайный файл.")
            return

    caption, markup, media_input, new_page = await build_album_page(state, bot, query.from_user.id)

    if media_input:
        await state.update_data(page=new_page)
        try:
            await bot.edit_message_media(
                media=media_input,
                chat_id=query.message.chat.id,
                message_id=query.message.message_id,
                reply_markup=markup
            )

            await bot.edit_message_caption(
                 chat_id=query.message.chat.id,
                 message_id=query.message.message_id,
                 caption=caption,
                 reply_markup=markup
            )
        except TelegramBadRequest as e:
            if "message is not modified" in str(e):
                 pass
            elif "message can't be edited" in str(e):
                 logger.warning(f"Сообщение {query.message.message_id} слишком старое для редактирования.")
                 await query.message.answer("Сообщение слишком старое, не могу обновить.")
            elif "message to edit not found" in str(e):
                 logger.warning(f"Сообщение {query.message.message_id} не найдено для редактирования.")
            else:
                logger.error(f"Ошибка обновления медиа в альбоме: {e}")
                try:
                    await query.message.answer("Не удалось обновить предыдущее сообщение, показываю текущий файл.")
                    if isinstance(media_input, InputMediaPhoto):
                        await query.message.answer_photo(media_input.media.file, caption=caption, reply_markup=markup)
                    else:
                        await query.message.answer_video(media_input.media.file, caption=caption, reply_markup=markup)
                except Exception as send_err:
                     logger.error(f"Не удалось даже отправить новое сообщение: {send_err}")

    else:
        try:
            await query.message.edit_text(caption, reply_markup=markup)
        except TelegramBadRequest as e:
             if "message is not modified" not in str(e):
                logger.error(f"Ошибка обновления текста в альбоме: {e}")


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
            text += f"\n*Статус:* 🎁 Задание принято, будут кары если не выполните\\!\n"
            builder.button(text="🎁 Молить об отмене", callback_data=f"wishlist_unbook:{item_id}")
        else:
            text += f"\n*Статус:* ⛔️ Захвачено {booker_id}\n"
            builder.button(text="⛔️ Захвачен", callback_data="wishlist_noop")
    elif is_owner:
        text += f"\n*Статус:* ✅ Ваш товар\\. Доступен для приватизации\n"
        builder.button(text="🗑️ Отдать африканским детям", callback_data=f"wishlist_delete:{item_id}")
    else:
        text += f"\n*Статус:* ✅ Доступен для захвата\n"
        builder.button(text="🎁 Приватизировать", callback_data=f"wishlist_book:{item_id}")

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

        await query.answer("✅ Теперь живи с этим!")
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

        await query.answer("✅ Мольбы услышаны!")
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
            await query.message.edit_text("✅ Товар удален. Желать больше нечего...", reply_markup=None)
            await query.answer("Товар удален... Желать больше нечего...", show_alert=True)
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