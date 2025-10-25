import logging
from datetime import datetime
import re
from aiogram import Bot
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest

from app.kafka.producer import kafka_producer
from app.bot.states import WishlistAddManual
from app.bot.utils import escape_markdown, reset_to_main_menu
from app.bot.keyboards import build_menu_keyboard

logger = logging.getLogger(__name__)


async def wishlist_add_url_handler(query: CallbackQuery, state: FSMContext):
    await query.answer()
    action, booking_type, *text_parts = query.data.split(":", 2)

    if booking_type == "cancel":
        await query.message.edit_text("Отменено.")
        return

    source_text = ":".join(text_parts)
    is_infinitely_bookable = (booking_type == "infinite")

    payload = {
        "source_text": source_text,
        "telegram_id": query.from_user.id,
        "is_infinitely_bookable": is_infinitely_bookable
    }

    try:
        await kafka_producer.send("wishlist.wishlist.add", payload)
        await query.message.edit_text("Отлично! Отправил ссылку на обработку. Я пришлю уведомление, когда закончу.")
    except Exception as e:
        logger.error(f"Kafka error on 'wishlist.wishlist.add': {e}")
        await query.message.edit_text("Ошибка. Не смог отправить в Kafka.")


async def process_manual_wishlist_name(message: Message, state: FSMContext):
    if not message.text:
        await message.answer("Нужно название.")
        return
    data = await state.get_data()
    if "manual_add_data" not in data: data["manual_add_data"] = {}
    data["manual_add_data"]["name"] = message.text
    await state.set_data(data)
    await state.set_state(WishlistAddManual.waiting_for_cost)
    await message.answer("Теперь введи примерную цену (или 'пропустить'):")


async def process_manual_wishlist_cost(message: Message, state: FSMContext):
    data = await state.get_data()
    if "manual_add_data" not in data: data["manual_add_data"] = {}
    if message.text and message.text.lower() != 'пропустить':
        data["manual_add_data"]["cost"] = message.text
    await state.set_data(data)
    await state.set_state(WishlistAddManual.waiting_for_url)
    await message.answer("Добавь ссылку (или 'пропустить'):")


async def process_manual_wishlist_url(message: Message, state: FSMContext):
    data = await state.get_data()
    if "manual_add_data" not in data: data["manual_add_data"] = {}
    if message.text and message.text.lower() != 'пропустить':
        data["manual_add_data"]["item_url"] = message.text

    await state.set_data(data)
    await state.set_state(WishlistAddManual.confirming)

    item = data.get("manual_add_data", {})
    text = f"Проверь:\n"
    text += f"Название: {escape_markdown(item.get('name', 'N/A'))}\n"
    text += f"Цена: {escape_markdown(item.get('cost', 'N/A'))}\n"
    text += f"URL: {escape_markdown(item.get('item_url', 'N/A'))}\n"

    builder = InlineKeyboardBuilder()
    builder.button(text="🎁 Обычный (1 бронь)", callback_data="wishlist_manual_confirm:single")
    builder.button(text="♾️ Общий (много броней)", callback_data="wishlist_manual_confirm:infinite")
    builder.button(text="❌ Отмена", callback_data="wishlist_manual_confirm:cancel")
    await message.answer(text, reply_markup=builder.as_markup(), parse_mode=None)


async def process_manual_wishlist_confirm(query: CallbackQuery, state: FSMContext):
    await query.answer()
    action, booking_type = query.data.split(":")

    if booking_type == "cancel":
        await query.message.edit_text("Отменено.")
        await state.clear()
        await reset_to_main_menu(query.message, state)
        return

    data = await state.get_data()
    payload = data.get("manual_add_data", {})
    if not payload.get("name"):
        await query.message.edit_text("Ошибка. Нет имени. Начни заново.")
        await state.clear()
        await reset_to_main_menu(query.message, state)
        return

    payload["is_infinitely_bookable"] = (booking_type == "infinite")

    try:
        await kafka_producer.send("wishlist.item.add_manual", payload)
        await query.message.edit_text("Сохранил!")
    except Exception as e:
        logger.error(f"Kafka error on 'wishlist.item.add_manual': {e}")
        await query.message.edit_text("Ошибка. Не смог отправить в Kafka.")

    await state.clear()
    await reset_to_main_menu(query.message, state)


async def build_wishlist_page(state: FSMContext, viewer_user_id: int) -> tuple[str, InlineKeyboardMarkup | None]:
    data = await state.get_data()
    items = data.get("items", [])
    current_index = data.get("current_index", 0)
    owner_user_id = data.get("owner_user_id", 0)

    if not items or current_index >= len(items):
        logger.warning("Wishlist empty/index out of bounds.")
        return "Вишлист пуст\\.", None

    item = items[current_index]

    item_price_raw = str(item.get("cost", "N/A"))
    item_delivery_raw = str(item.get("delivery_date", "N/A"))
    item_name_raw = str(item.get("name", "N/A"))
    item_url_raw = item.get("item_url")

    item_price = escape_markdown(item_price_raw)
    item_delivery = escape_markdown(item_delivery_raw)
    item_name = escape_markdown(item_name_raw)

    item_url_markdown = escape_markdown("N/A")
    if item_url_raw:
        safe_url_content = escape_markdown(item_url_raw)  # Экранируем весь URL
        item_url_markdown = f"[Link]({safe_url_content})"

    is_infinitely_bookable = item.get("is_infinitely_bookable", False)
    bookings = item.get("bookings", [])

    text = f"*Товар {current_index + 1}/{len(items)}*\n\n*Название:* {item_name}\n"
    text += f"*URL:* {item_url_markdown}\n"

    is_owner = (owner_user_id == viewer_user_id)
    text += f"*Цена:* {item_price}\n*Доставка:* {item_delivery}\n"

    if is_infinitely_bookable:
        text += "*Тип:* ♾️ Общий (можно бронить многим)\n"

    builder = InlineKeyboardBuilder()
    nav_buttons = []

    if current_index > 0:
        nav_buttons.append(InlineKeyboardButton(text="⬅️ Назад", callback_data="wishlist_prev"))
    nav_buttons.append(InlineKeyboardButton(text="❌ Закрыть", callback_data="wishlist_close"))
    if current_index < len(items) - 1:
        nav_buttons.append(InlineKeyboardButton(text="Вперед ➡️", callback_data="wishlist_next"))

    builder.row(*nav_buttons)
    item_id = item.get('item_id', 'unknown')

    my_booking = next((b for b in bookings if b.get("booked_by_user_id") == viewer_user_id), None)

    if is_owner:
        text += f"\n*Статус:* ✅ Ваш товар\\. Доступен для приватизации\n"
        if bookings:
            text += f"*Забронено:* {len(bookings)} раз\n"
        builder.button(text="🗑️ Отдать африканским детям", callback_data=f"wishlist_delete:{item_id}")
    elif my_booking:
        text += f"\n*Статус:* 🎁 Задание принято, будут кары если не выполните\\!\n"
        if is_infinitely_bookable and len(bookings) > 1:
            text += f"*Также забронено:* {len(bookings) - 1} другими\n"
        builder.button(text="🎁 Молить об отмене", callback_data=f"wishlist_unbook:{item_id}")
    elif not is_infinitely_bookable and bookings:
        text += f"\n*Статус:* ⛔️ Захвачено кем\\-то другим\n"
        builder.button(text="⛔️ Захвачен", callback_data="wishlist_noop")
    else:
        text += f"\n*Статус:* ✅ Доступен для захвата\n"
        if is_infinitely_bookable and bookings:
            text += f"*Уже забронено:* {len(bookings)} раз\n"
        builder.button(text="🎁 Приватизировать", callback_data=f"wishlist_book:{item_id}")

    return text, builder.as_markup()


async def wishlist_navigation_handler(query: CallbackQuery, state: FSMContext, bot: Bot):
    await query.answer()
    action, *value = query.data.split(":")
    data = await state.get_data()
    items = data.get("items", [])
    current_index = data.get("current_index", 0)
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
        return
    elif action == "wishlist_book":
        item_id = int(value[0])
        await kafka_producer.send("wishlist.item.book", {"item_id": item_id, "booker_user_id": query.from_user.id})
        logger.info(f"{query.from_user.id} booked {item_id}")
        if items and 0 <= current_index < len(items):
            new_booking_info = {'booked_by_user_id': query.from_user.id,
                                'booked_at': datetime.utcnow().isoformat()}
            if 'bookings' not in items[current_index] or items[current_index]['bookings'] is None:
                items[current_index]['bookings'] = []
            items[current_index]['bookings'].append(new_booking_info)
            await state.update_data(items=items)

        try:
            text, markup = await build_wishlist_page(state, query.from_user.id)
            await query.message.edit_text(text, reply_markup=markup, parse_mode="MarkdownV2",
                                          disable_web_page_preview=False)
        except TelegramBadRequest as e:
            logger.error(f"Failed to edit message on book (MarkdownV2): {e}. Problematic text:\n>>>\n{text}\n<<<")
            try:
                plain_text = re.sub(r'\\([_*\[\]()~`>#\+\-=|{}.!])', r'\1', text)
                plain_text = plain_text.replace('*', '').replace('_', '')
                await query.message.edit_text(plain_text, reply_markup=markup, disable_web_page_preview=False)
            except Exception as plain_e:
                logger.error(f"Failed to edit message on book even as plain text: {plain_e}")
                await query.message.answer(f"Ошибка отображения: {e}")
        except Exception as e:
            logger.error(f"Unexpected error editing message on book: {e}", exc_info=True)
            await query.message.answer(f"Неожиданная ошибка отображения: {e}")

        await query.answer("✅ Теперь живи с этим!")
        return

    elif action == "wishlist_unbook":
        item_id = int(value[0])
        await kafka_producer.send("wishlist.item.unbook", {"item_id": item_id, "unbooker_user_id": query.from_user.id})
        logger.info(f"{query.from_user.id} unbooked {item_id}")
        if items and 0 <= current_index < len(items):
            items[current_index]['bookings'] = [
                b for b in items[current_index].get('bookings', [])
                if b.get('booked_by_user_id') != query.from_user.id
            ]
            await state.update_data(items=items)

        try:
            text, markup = await build_wishlist_page(state, query.from_user.id)
            await query.message.edit_text(text, reply_markup=markup, parse_mode="MarkdownV2",
                                          disable_web_page_preview=False)
        except TelegramBadRequest as e:
            logger.error(f"Failed to edit message on unbook (MarkdownV2): {e}. Problematic text:\n>>>\n{text}\n<<<")
            try:
                plain_text = re.sub(r'\\([_*\[\]()~`>#\+\-=|{}.!])', r'\1', text)
                plain_text = plain_text.replace('*', '').replace('_', '')
                await query.message.edit_text(plain_text, reply_markup=markup, disable_web_page_preview=False)
            except Exception as plain_e:
                logger.error(f"Failed to edit message on unbook even as plain text: {plain_e}")
                await query.message.answer(f"Ошибка отображения: {e}")
        except Exception as e:
            logger.error(f"Unexpected error editing message on unbook: {e}", exc_info=True)
            await query.message.answer(f"Неожиданная ошибка отображения: {e}")

        await query.answer("✅ Мольбы услышаны!")
        return

    elif action == "wishlist_delete":
        item_id = int(value[0])
        await kafka_producer.send("wishlist.item.delete", {"item_id": item_id, "deleter_user_id": query.from_user.id})
        logger.info(f"{query.from_user.id} deleted {item_id}")
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

        try:
            text, markup = await build_wishlist_page(state, query.from_user.id)
            await query.message.edit_text(text, reply_markup=markup, parse_mode="MarkdownV2",
                                          disable_web_page_preview=False)
        except TelegramBadRequest as e:
            logger.error(f"Failed to edit message on delete (MarkdownV2): {e}. Problematic text:\n>>>\n{text}\n<<<")
            try:
                plain_text = re.sub(r'\\([_*\[\]()~`>#\+\-=|{}.!])', r'\1', text)
                plain_text = plain_text.replace('*', '').replace('_', '')
                await query.message.edit_text(plain_text, reply_markup=markup, disable_web_page_preview=False)
            except Exception as plain_e:
                logger.error(f"Failed to edit message on delete even as plain text: {plain_e}")
                await query.message.answer(f"Ошибка отображения: {e}")
        except Exception as e:
            logger.error(f"Unexpected error editing message on delete: {e}", exc_info=True)
            await query.message.answer(f"Неожиданная ошибка отображения: {e}")

        await query.answer("✅ Товар удален!")
        return

    elif action == "wishlist_noop":
        await query.answer()
        return

    try:
        text, markup = await build_wishlist_page(state, query.from_user.id)
        if text and markup:
            await query.message.edit_text(text, reply_markup=markup, parse_mode="MarkdownV2",
                                          disable_web_page_preview=False)
        elif text:
            await query.message.edit_text(text, parse_mode="MarkdownV2", disable_web_page_preview=False)
    except TelegramBadRequest as e:
        logger.error(f"Failed to edit message on navigation (MarkdownV2): {e}. Problematic text:\n>>>\n{text}\n<<<")
        try:
            plain_text = re.sub(r'\\([_*\[\]()~`>#\+\-=|{}.!])', r'\1', text)
            plain_text = plain_text.replace('*', '').replace('_', '')
            if markup:
                await query.message.edit_text(plain_text, reply_markup=markup, disable_web_page_preview=False)
            else:
                await query.message.edit_text(plain_text, disable_web_page_preview=False)
        except Exception as plain_e:
            logger.error(f"Failed to edit message on navigation even as plain text: {plain_e}")
            await query.message.answer(f"Ошибка отображения: {e}")
    except Exception as e:
        logger.error(f"Unexpected error editing message on navigation: {e}", exc_info=True)
        await query.message.answer(f"Неожиданная ошибка отображения: {e}")


async def all_wishlists_navigation_handler(query: CallbackQuery, state: FSMContext, bot: Bot):
    await query.answer()
    action, *value = query.data.split(":", 1)

    if action == "all_wishlists_close":
        await query.message.delete()
        await state.clear()
        return

    if action == "all_wishlists_select":
        owner_name = value[0]
        requester_id = query.from_user.id
        logger.info(f"User {requester_id} requests wishlist for owner '{owner_name}'")
        try:
            await kafka_producer.send("wishlist.view.viewer", {
                "owner_user_name": owner_name,
                "requester_user_id": requester_id,
                "telegram_id": requester_id
            })
            await query.message.edit_text(f"Загружаю вишлист для *{escape_markdown(owner_name)}*",
                                          parse_mode="MarkdownV2", reply_markup=None)
        except Exception as e:
            logger.error(f"Kafka error on 'wishlist.view.viewer': {e}")
            await query.message.answer("Не удалось запросить вишлист.")