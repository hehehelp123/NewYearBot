import logging
from datetime import datetime
import re
from aiogram import Bot
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest

from app.kafka.producer import kafka_producer
from app.bot.states import WishlistAddManual, BookedItemsBrowser
from app.bot.utils import escape_markdown, reset_to_main_menu
from app.bot.keyboards import build_menu_keyboard
from app.kafka.consumer import CLOSE_WISHLIST_BUTTON, CLOSE_BOOKED_BUTTON

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
    text += f"Название: {item.get('name', 'N/A')}\n"
    text += f"Цена: {item.get('cost', 'N/A')}\n"
    text += f"URL: {item.get('item_url', 'N/A')}\n"

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
        return "Вишлист пуст.", None

    item = items[current_index]

    item_price_raw = str(item.get("cost", "N/A"))
    item_delivery_raw = str(item.get("delivery_date", "N/A"))
    item_name_raw = str(item.get("name", "N/A"))
    item_url_raw = str(item.get("item_url", "N/A"))

    is_infinitely_bookable = item.get("is_infinitely_bookable", False)
    bookings = item.get("bookings", [])

    text = f"Товар {current_index + 1}/{len(items)}\n\nНазвание: {item_name_raw}\n"
    text += f"URL: {item_url_raw}\n"
    is_owner = (owner_user_id == viewer_user_id)
    text += f"Цена: {item_price_raw}\nДоставка: {item_delivery_raw}\n"

    if is_infinitely_bookable:
        text += "Тип: ♾️ Общий (можно бронить многим)\n"

    builder = InlineKeyboardBuilder()
    nav_buttons = []

    if current_index > 0:
        nav_buttons.append(InlineKeyboardButton(text="⬅️ Назад", callback_data="wishlist_prev"))
    # Убираем кнопку Закрыть из Inline, т.к. есть Reply
    # nav_buttons.append(InlineKeyboardButton(text="❌ Закрыть", callback_data="wishlist_close"))
    if current_index < len(items) - 1:
        nav_buttons.append(InlineKeyboardButton(text="Вперед ➡️", callback_data="wishlist_next"))

    if nav_buttons:  # Добавляем ряд только если есть кнопки навигации
        builder.row(*nav_buttons)

    item_id = item.get('item_id', 'unknown')

    my_booking = next((b for b in bookings if b.get("booked_by_user_id") == viewer_user_id), None)

    if not is_owner:
        if my_booking:
            text += f"\nСтатус: 🎁 Забронено тобой!\n"
            if is_infinitely_bookable and len(bookings) > 1:
                text += f"Также забронено: {len(bookings) - 1} другими\n"
            builder.button(text="🎁 Снять бронь", callback_data=f"wishlist_unbook:{item_id}")
        elif not is_infinitely_bookable and bookings:
            text += f"\nСтатус: ⛔️ Забронено кем-то другим\n"
            builder.button(text="⛔️ Забронен", callback_data="wishlist_noop")
        else:
            text += f"\nСтатус: ✅ Доступен\n"
            if is_infinitely_bookable and bookings:
                text += f"Уже забронено: {len(bookings)} раз\n"
            builder.button(text="🎁 Забронить", callback_data=f"wishlist_book:{item_id}")
    else:
        builder.button(text="🗑️ Удалить", callback_data=f"wishlist_delete:{item_id}")

    return text, builder.as_markup() if builder._buttons else None


async def wishlist_navigation_handler(query: CallbackQuery, state: FSMContext, bot: Bot):
    await query.answer()
    action, *value = query.data.split(":")
    data = await state.get_data()
    items = data.get("items", [])
    current_index = data.get("current_index", 0)
    new_index = current_index

    # Кнопка Закрыть в ReplyKeyboard обрабатывается в menu_handler
    # if action == "wishlist_close":
    #     await query.message.delete()
    #     await reset_to_main_menu(query.message, state)
    #     return

    if action == "wishlist_next":
        if current_index < len(items) - 1:
            new_index = current_index + 1
            await state.update_data(current_index=new_index)
    elif action == "wishlist_prev":
        if current_index > 0:
            new_index = current_index - 1
            await state.update_data(current_index=new_index)
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
        # Обновляем сообщение после изменения состояния
        text, markup = await build_wishlist_page(state, query.from_user.id)
        try:
            await query.message.edit_text(text, reply_markup=markup, parse_mode=None, disable_web_page_preview=False)
        except Exception as e:
            logger.error(f"Failed to edit message on book: {e}. Text: {text}")
            await query.message.answer(f"Ошибка отображения: {e}")
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
        # Обновляем сообщение после изменения состояния    
        text, markup = await build_wishlist_page(state, query.from_user.id)
        try:
            await query.message.edit_text(text, reply_markup=markup, parse_mode=None, disable_web_page_preview=False)
        except Exception as e:
            logger.error(f"Failed to edit message on unbook: {e}. Text: {text}")
            await query.message.answer(f"Ошибка отображения: {e}")
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
        # Обновляем сообщение после изменения состояния
        text, markup = await build_wishlist_page(state, query.from_user.id)
        try:
            await query.message.edit_text(text, reply_markup=markup, parse_mode=None, disable_web_page_preview=False)
        except Exception as e:
            logger.error(f"Failed to edit message on delete: {e}. Text: {text}")
            await query.message.answer(f"Ошибка отображения: {e}")
        await query.answer("✅ Товар удален!")
        return

    elif action == "wishlist_noop":
        await query.answer()
        return

    # Навигация (prev/next) - только если индекс изменился
    if new_index != current_index:
        try:
            text, markup = await build_wishlist_page(state, query.from_user.id)
            if text and markup:
                await query.message.edit_text(text, reply_markup=markup, parse_mode=None,
                                              disable_web_page_preview=False)
            elif text:
                await query.message.edit_text(text, parse_mode=None, disable_web_page_preview=False)
        except Exception as e:
            logger.error(f"Failed to edit message on navigation: {e}. Text: {text}")
            await query.message.answer(f"Ошибка отображения: {e}")


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
            await query.message.edit_text(f"Загружаю вишлист для {owner_name}",
                                          parse_mode=None, reply_markup=None)
        except Exception as e:
            logger.error(f"Kafka error on 'wishlist.view.viewer': {e}")
            await query.message.answer("Не удалось запросить вишлист.")


async def build_booked_item_page(state: FSMContext, viewer_user_id: int) -> tuple[str, InlineKeyboardMarkup | None]:
    data = await state.get_data()
    items = data.get("booked_items", [])
    current_index = data.get("current_index", 0)

    if not items or current_index >= len(items):
        logger.warning("Booked items list empty/index out of bounds.")
        return "Список броней пуст.", None

    item = items[current_index]

    item_price_raw = str(item.get("cost", "N/A"))
    item_delivery_raw = str(item.get("delivery_date", "N/A"))
    item_name_raw = str(item.get("name", "N/A"))
    item_url_raw = str(item.get("item_url", "N/A"))
    owner_name_raw = str(item.get("owner_name", "Неизвестно"))

    text = f"Бронь {current_index + 1}/{len(items)}\n\nНазвание: {item_name_raw}\n"
    text += f"У: {owner_name_raw}\n"
    text += f"URL: {item_url_raw}\n"
    text += f"Цена: {item_price_raw}\nДоставка: {item_delivery_raw}\n"

    builder = InlineKeyboardBuilder()
    nav_buttons = []

    if current_index > 0:
        nav_buttons.append(InlineKeyboardButton(text="⬅️", callback_data="booked_prev"))
    # Убираем кнопку Закрыть из Inline
    # nav_buttons.append(InlineKeyboardButton(text="❌", callback_data="booked_close"))
    if current_index < len(items) - 1:
        nav_buttons.append(InlineKeyboardButton(text="➡️", callback_data="booked_next"))

    if nav_buttons:
        builder.row(*nav_buttons)

    item_id = item.get('item_id', 'unknown')
    builder.button(text="🎁 Снять бронь", callback_data=f"unbook_booked_item:{item_id}")

    return text, builder.as_markup() if builder._buttons else None


async def booked_items_navigation_handler(query: CallbackQuery, state: FSMContext, bot: Bot):
    await query.answer()
    action, *value = query.data.split(":")
    data = await state.get_data()
    items = data.get("booked_items", [])
    current_index = data.get("current_index", 0)
    new_index = current_index

    # Кнопка Закрыть в ReplyKeyboard обрабатывается в menu_handler
    # if action == "booked_close":
    #     await query.message.delete()
    #     await reset_to_main_menu(query.message, state)
    #     return

    if action == "booked_next":
        if current_index < len(items) - 1:
            new_index = current_index + 1
            await state.update_data(current_index=new_index)
    elif action == "booked_prev":
        if current_index > 0:
            new_index = current_index - 1
            await state.update_data(current_index=new_index)
    elif action == "unbook_booked_item":
        item_id = int(value[0])
        await kafka_producer.send("wishlist.item.unbook", {"item_id": item_id, "unbooker_user_id": query.from_user.id})
        logger.info(f"{query.from_user.id} unbooked item {item_id} from booked list")

        new_items = [item for item in items if item['item_id'] != item_id]
        if not new_items:
            await state.update_data(booked_items=[], current_index=0)
            await query.message.edit_text("✅ Бронь снята. Список пуст.", reply_markup=None)
            await query.answer("Бронь снята!")
            return

        new_index = current_index
        if new_index >= len(new_items):
            new_index = max(0, len(new_items) - 1)
        await state.update_data(booked_items=new_items, current_index=new_index)

        try:
            text, markup = await build_booked_item_page(state, query.from_user.id)
            await query.message.edit_text(text, reply_markup=markup, parse_mode=None, disable_web_page_preview=False)
        except Exception as e:
            logger.error(f"Failed to edit message on unbook from booked list: {e}. Text: {text}")
            await query.message.answer(f"Ошибка отображения: {e}")
        await query.answer("✅ Бронь снята!")
        return

    if new_index != current_index:
        try:
            text, markup = await build_booked_item_page(state, query.from_user.id)
            if text and markup:
                await query.message.edit_text(text, reply_markup=markup, parse_mode=None,
                                              disable_web_page_preview=False)
            elif text:
                await query.message.edit_text(text, parse_mode=None, disable_web_page_preview=False)
        except Exception as e:
            logger.error(f"Failed to edit message on booked navigation: {e}. Text: {text}")
            await query.message.answer(f"Ошибка отображения: {e}")