import logging
from aiogram import Bot
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.fsm.context import FSMContext

from app.services.storage_service import storage_service
from app.bot.states import MediaUpload
from app.bot.constants import STOP_UPLOAD_BUTTON
from app.bot.utils import get_current_new_year, get_media_folder, reset_to_main_menu
from app.bot.keyboards import build_menu_keyboard

logger = logging.getLogger(__name__)


async def start_media_upload_handler(message: Message, state: FSMContext):
    logger.info(f"{message.from_user.id} started media upload.")
    current_year = get_current_new_year()
    kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text=STOP_UPLOAD_BUTTON)]], resize_keyboard=True)
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
            reply_markup=build_menu_keyboard([], add_start=True),
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
    logger.info(f"{message.from_user.id} chose year {year}.")
    await state.set_state(MediaUpload.uploading)
    await state.update_data(year=year)
    kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text=STOP_UPLOAD_BUTTON)]], resize_keyboard=True)
    await message.answer(
        f"Режим загрузки фото/видео для **{year}** включен.\n"
        "Отправляйте нюдсы. Когда надоест, так и скажите.",
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
    year = data.get("year")
    if not year:
        logger.warning(f"No year in FSM for {message.from_user.id}.")
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
    await message.answer(f"Украдываю (ID: ...{file_unique_id[-6:]})...")
    try:
        file_info = await bot.get_file(file_id)
        file_bytes_io = await bot.download_file(file_info.file_path)
        folder = get_media_folder(year)
        object_name = storage_service.upload_file(file_bytes_io.read(), original_filename, folder=folder,
                                                  content_type=content_type)
        logger.info(f"Uploaded {object_name} to {folder}")
        await message.answer(f"✅ Файл ...{file_unique_id[-6:]} национализирован!")
    except Exception as e:
        logger.error(f"Upload error {file_unique_id}: {e}", exc_info=True)
        await message.answer(f"❌ Што ты наделал ...{file_unique_id[-6:]}.")