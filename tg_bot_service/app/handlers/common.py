import logging
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder

from app.core.config import settings
from app.kafka.producer import kafka_producer
from app.middlewares.access_middleware import allowed_user_ids
from app.bot.constants import WELCOME_IMAGE_FILE_ID, WELCOME_TEXT, UPLOAD_MEDIA_BUTTON, VIEW_ALBUMS_BUTTON
from app.bot.keyboards import welcome_keyboard, build_menu_keyboard
from app.bot.utils import load_schema, get_root_items, escape_markdown

logger = logging.getLogger(__name__)


async def start_handler(message: Message, state: FSMContext) -> None:
    await load_schema(flag=1)
    await state.clear()
    user = message.from_user
    logger.info(f"/start от {user.id} ({user.username})")
    if user:
        user_data = {"telegram_id": user.id, "username": user.username or user.full_name}
        try:
            await kafka_producer.send("user.user.create", user_data)
            await kafka_producer.send("wishlist.wishlist.create", user_data)
            logger.info(f"Kafka create events sent for {user.id}")
        except Exception as e:
            logger.error(f"Kafka error on /start for {user.id}: {e}")

    is_admin = user.id in settings.ADMIN_TELEGRAM_IDS
    if not is_admin and user.id not in allowed_user_ids:
        logger.warning(f"{user.id} tried /start but not allowed.")
        await message.answer("Привет! Для доступа к боту обратись к администратору.")
        return

    kb = await welcome_keyboard()
    if WELCOME_IMAGE_FILE_ID == "PASTE_YOUR_FILE_ID_HERE":
        logger.warning("WELCOME_IMAGE_FILE_ID не установлен!")
        await message.answer(WELCOME_TEXT, reply_markup=kb)
        return
    try:
        await message.answer_photo(photo=WELCOME_IMAGE_FILE_ID, caption=WELCOME_TEXT, reply_markup=kb)
    except Exception as e:
        logger.error(f"Failed photo send: {e}.")
        await message.answer(WELCOME_TEXT, reply_markup=kb)


async def start_button_handler(message: Message, state: FSMContext) -> None:
    logger.debug(f"'Куда я жмав' от {message.from_user.id}")
    await state.clear()
    user_id = message.from_user.id
    item_names = await get_root_items(user_id)
    item_names.extend([UPLOAD_MEDIA_BUTTON, VIEW_ALBUMS_BUTTON])
    kb = build_menu_keyboard(item_names, add_start=False, add_back_to_welcome=True)
    schema = await load_schema()
    await state.update_data(current_node=schema)
    await message.answer("Ну кликни шо-нить:", reply_markup=kb)


async def back_to_welcome_handler(message: Message, state: FSMContext):
    logger.debug(f"Back to welcome for {message.from_user.id}")
    await start_handler(message, state)


async def show_main_menu_callback(query: CallbackQuery, state: FSMContext):
    await query.answer()
    user_id = query.from_user.id
    item_names = await get_root_items(user_id)
    item_names.extend([UPLOAD_MEDIA_BUTTON, VIEW_ALBUMS_BUTTON])
    if item_names:
        schema = await load_schema()
        await state.update_data(current_node=schema)
        kb = build_menu_keyboard(item_names, add_start=False, add_back_to_welcome=True)
        await query.message.answer("Доброе утро, мопсы! Выберите пункт меню (или просто посмотрите какие кайфовые тут смайлики, долго выбирал):", reply_markup=kb)
    else:
        await query.message.answer("Схема меню пуста или не найдена.")


async def show_info_callback(query: CallbackQuery):
    action = query.data.split(":")[-1]
    if action == "wifi":
        logger.debug(f"Пользователь {query.from_user.id} запросил WiFi")
        password = escape_markdown(settings.WIFI_PASSWORD)
        await query.message.answer(f"Пароль от WiFi:\n\n`{password}`", parse_mode="MarkdownV2")
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
        await query.message.edit_caption(caption="Жабы со стажем, все вопросы к жабам (за экзистенциальные будете наказаны).", reply_markup=builder.as_markup())
        await query.answer()
    elif action == "back_to_welcome":
        logger.debug(f"Пользователь {query.from_user.id} вернулся в стартовое меню")
        kb = await welcome_keyboard()
        await query.message.edit_caption(caption=WELCOME_TEXT, reply_markup=kb)
        await query.answer()