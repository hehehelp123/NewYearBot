import logging
from aiogram import Bot
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InputMediaPhoto, InputMediaVideo, \
    BufferedInputFile
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest

from app.core.config import settings
from app.core.http_client import http_client
from app.bot.states import AlbumBrowser

logger = logging.getLogger(__name__)


async def start_album_view_handler(message: Message, state: FSMContext):
    logger.debug(f"{message.from_user.id} requests albums.")
    await state.clear()
    try:
        response = await http_client.client.get(f"{settings.ORCHESTRATOR_URL}/api/v1/albums")
        response.raise_for_status()
        albums = response.json()
        if not albums:
            await message.answer("Пока нет ни одного загруженного альбома (базе пиздец):(")
            return
        await state.set_state(AlbumBrowser.choosing_album)
        builder = InlineKeyboardBuilder()
        for album in albums:
            builder.button(text=f"Альбом: {album}", callback_data=f"album_view:{album}")
        builder.button(text="❌ Не, нафиг", callback_data="album_close")
        builder.adjust(1)
        await message.answer("Выберите альбом для просмотра:", reply_markup=builder.as_markup())
    except Exception as e:
        logger.error(f"Get albums failed: {e}")
        await message.answer("Не удалось загрузить список альбомов. Попробуйте написать Мише.")


async def build_album_page(state: FSMContext, bot: Bot, chat_id: int):
    data = await state.get_data()
    album_id = data.get("album_id")
    page = data.get("page", 1)

    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="Выбирай чо хошь", callback_data="album_menu"),
                InlineKeyboardButton(text="❌ Не-не-не", callback_data="album_close"))
    error_markup = builder.as_markup()

    try:
        response = await http_client.client.get(f"{settings.ORCHESTRATOR_URL}/api/v1/albums/{album_id}",
                                                params={"page": page, "page_size": 1})
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
        caption = f"Альбом {album_id} | Файл {current_page} из {total_pages}"

        builder = InlineKeyboardBuilder()
        nav_buttons = []

        if current_page > 1:
            nav_buttons.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"album_page:{current_page - 1}"))
        nav_buttons.append(InlineKeyboardButton(text="🎲 Поиграем?~", callback_data="album_random"))
        if current_page < total_pages:
            nav_buttons.append(InlineKeyboardButton(text="Вперед ➡️", callback_data=f"album_page:{current_page + 1}"))

        builder.row(*nav_buttons)
        builder.row(InlineKeyboardButton(text="Меню альбомов", callback_data="album_menu"),
                    InlineKeyboardButton(text="❌ Закрыть", callback_data="album_close"))
        input_file = BufferedInputFile(media_bytes, filename=filename)
        media_input = InputMediaPhoto(media=input_file) if media_type == "photo" else InputMediaVideo(media=input_file)
        return caption, builder.as_markup(), media_input, current_page
    except Exception as e:
        logger.error(f"Build page error: {e}")
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

    if action == "album_view":
        album_id = value[0]
        await state.update_data(album_id=album_id, page=1)
    elif action == "album_page":
        page = int(value[0])
        await state.update_data(page=page)
    elif action == "album_random":
        data = await state.get_data()
        album_id = data.get("album_id")
        try:
            response = await http_client.client.get(f"{settings.ORCHESTRATOR_URL}/api/v1/albums/{album_id}/random")
            response.raise_for_status()
            random_data = response.json()
            new_page = random_data.get("page")
            if new_page: await state.update_data(page=new_page)
        except Exception as e:
            logger.error(f"Random error: {e}")
            await query.message.answer("Не удалось загрузить случайный файл.")
            return

    caption, markup, media_input, new_page = await build_album_page(state, bot, query.from_user.id)

    if media_input:
        await state.update_data(page=new_page)
        try:
            await bot.edit_message_media(media=media_input, chat_id=query.message.chat.id,
                                         message_id=query.message.message_id, reply_markup=markup)
            await bot.edit_message_caption(chat_id=query.message.chat.id, message_id=query.message.message_id,
                                           caption=caption, reply_markup=markup)
        except TelegramBadRequest as e:
            if "not modified" in str(e):
                pass
            elif "can't be edited" in str(e):
                logger.warning(f"Сообщение {query.message.message_id} слишком старое для редактирования.")
                await query.message.answer("Сообщение слишком старое, не могу обновить.")
            elif "not found" in str(e):
                logger.warning(f"Сообщение {query.message.message_id} не найдено для редактирования.")
            else:
                logger.error(f"Edit media/caption error: {e}")
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
            if "not modified" not in str(e): logger.error(f"Edit text error: {e}")