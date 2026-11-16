import asyncio
import logging
from aiogram import Bot
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder

from app.core.config import settings
from app.core.http_client import http_client
from app.services.storage_service import storage_service
from app.bot.states import MediaUpload
from app.bot.constants import STOP_UPLOAD_BUTTON
from app.bot.utils import get_media_folder, reset_to_main_menu
from app.bot.keyboards import build_menu_keyboard

logger = logging.getLogger(__name__)


async def start_media_upload_handler(message: Message, state: FSMContext):
    logger.info(f"{message.from_user.id} requests media upload.")
    await state.clear()
    try:
        response = await http_client.client.get(f"{settings.ORCHESTRATOR_URL}/api/v1/albums")
        response.raise_for_status()
        albums = response.json()

        await state.set_state(MediaUpload.choosing_album)
        builder = InlineKeyboardBuilder()

        for album in albums:
            builder.button(text=f"Альбом: {album}", callback_data=f"upload_album:{album}")

        is_admin = message.from_user.id in settings.ADMIN_TELEGRAM_IDS
        if is_admin:
            builder.button(text="➕ Создать новый альбом", callback_data="upload_album_new")

        builder.button(text="❌ Отмена", callback_data="upload_album_cancel")
        builder.adjust(1)

        await message.answer("Выберите альбом для загрузки фото/видео:", reply_markup=builder.as_markup())
    except Exception as e:
        logger.error(f"Get albums failed: {e}")
        await message.answer("Не удалось загрузить список альбомов. Попробуйте написать Мише.")


async def process_album_selection(query: CallbackQuery, state: FSMContext):
    await query.answer()
    action = query.data

    if action == "upload_album_cancel":
        await query.message.delete()
        await state.clear()
        return

    if action == "upload_album_new":
        await query.message.delete()
        await state.set_state(MediaUpload.waiting_for_new_album_name)
        await query.message.answer("Введите название нового альбома (лучше латиницей и без пробелов):",
                                   reply_markup=build_menu_keyboard([], add_start=True))
        return

    album_id = action.split(":")[1]
    await query.message.delete()
    await start_upload_for_album(query.message, state, album_id)


async def process_new_album_name(message: Message, state: FSMContext):
    if not message.text:
        await message.answer("Пожалуйста, отправьте текст.")
        return

    album_id = message.text.strip()
    logger.info(f"Admin {message.from_user.id} created new album target: {album_id}")
    await start_upload_for_album(message, state, album_id)


async def start_upload_for_album(message: Message, state: FSMContext, album_id: str):
    await state.set_state(MediaUpload.uploading)
    await state.update_data(album_id=album_id)
    kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text=STOP_UPLOAD_BUTTON)]], resize_keyboard=True)
    await message.answer(
        f"Режим загрузки фото/видео для альбома **{album_id}** включен.\n"
        "Отправляйте нюдсы. Когда закончите, нажмите кнопку внизу.",
        reply_markup=kb,
        parse_mode="Markdown"
    )


async def stop_media_upload_handler(message: Message, state: FSMContext):
    logger.info(f"{message.from_user.id} stopped media upload.")
    await state.clear()
    await message.answer("Неплохая работа.")
    await reset_to_main_menu(message, state)


async def media_upload_handler(message: Message, bot: Bot, state: FSMContext):
    if not (message.photo or message.video):
        await message.answer("Ну-ка, ну-ка, что тут у нас.")
        return
    data = await state.get_data()
    album_id = data.get("album_id")
    if not album_id:
        logger.warning(f"No album_id in FSM for {message.from_user.id}.")
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

    logger.debug(f"Got media {file_unique_id} from {message.from_user.id}")
    size = getattr(media, "file_size", None) or 0
    if size > TELEGRAM_DOWNLOAD_LIMIT:
        # Bot API не отдаёт ботам файлы больше 20 МБ — честно говорим об этом
        await _report(message, ok=False, reason=f"файл {size // (1024 * 1024)} МБ — Telegram не даёт ботам скачивать больше 20 МБ")
        return
    try:
        file_info = await bot.get_file(file_id)
        file_bytes_io = await bot.download_file(file_info.file_path, timeout=120)
        folder = get_media_folder(album_id)
        # MinIO-клиент синхронный: в отдельном потоке, чтобы не морозить бота для всех остальных
        object_name = await asyncio.to_thread(storage_service.upload_file, file_bytes_io.getvalue(),
                                              original_filename, folder, content_type)
        logger.info(f"Uploaded {object_name} to {folder}")
        await _report(message, ok=True)
    except Exception as e:
        logger.error(f"Upload error {file_unique_id}: {e}", exc_info=True)
        await _report(message, ok=False, reason="не смог сохранить")


TELEGRAM_DOWNLOAD_LIMIT = 20 * 1024 * 1024
_GROUP_DEBOUNCE_SECONDS = 2.5
_groups: dict[str, dict] = {}


async def _report(message: Message, ok: bool, reason: str | None = None):
    """Одиночное фото — один ответ. Альбом из N фото — один итоговый ответ, а не 2N сообщений
    (раньше бот упирался в лимиты Telegram на отправку и «пролагивал»)."""
    group_id = message.media_group_id
    if not group_id:
        if ok:
            await message.answer("✅ Национализировано!")
        else:
            await message.answer(f"❌ Што ты наделал: {reason}.")
        return

    g = _groups.setdefault(group_id, {"ok": 0, "failed": [], "task": None})
    if ok:
        g["ok"] += 1
    else:
        g["failed"].append(reason)
    if g["task"]:
        g["task"].cancel()
    g["task"] = asyncio.create_task(_flush_group(message, group_id))


async def _flush_group(message: Message, group_id: str):
    try:
        await asyncio.sleep(_GROUP_DEBOUNCE_SECONDS)
    except asyncio.CancelledError:
        return
    g = _groups.pop(group_id, None)
    if not g:
        return
    text = f"✅ Национализировано файлов: {g['ok']}"
    if g["failed"]:
        reasons = "; ".join(sorted(set(r for r in g["failed"] if r)))
        text += f"\n❌ Не вышло: {len(g['failed'])} ({reasons})"
    try:
        await message.answer(text)
    except Exception as e:
        logger.error(f"Group report failed: {e}")