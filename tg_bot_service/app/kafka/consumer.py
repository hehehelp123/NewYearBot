import asyncio
import json
import logging
import re
from datetime import datetime

from aiogram import Bot, Dispatcher
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.base import StorageKey
from aiogram.types import BufferedInputFile, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiokafka import AIOKafkaConsumer
from aiogram.exceptions import TelegramBadRequest

from app.middlewares.access_middleware import update_allowed_users
from app.core.config import settings
from app.services.storage_service import storage_service
from app.middlewares.access_middleware import update_allowed_users

from app.handlers.wishlist import build_wishlist_page
from app.bot.states import (
    WishlistBrowser,
    UserRemoval,
    AllWishlistsBrowser,
    WishlistAddManual
)
from app.bot.utils import escape_markdown

logger = logging.getLogger(__name__)


class KafkaBotConsumer:
    def __init__(self, bot: Bot, dp: Dispatcher, *topics: str):
        self.bot = bot
        self.topics = topics
        self.storage = dp.storage
        self.consumer: AIOKafkaConsumer | None = None
        self._task = None

    async def start(self):
        loop = asyncio.get_event_loop()
        self.consumer = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            loop=loop,
            group_id="tg_bot_group",
            auto_offset_reset='earliest',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        await self.consumer.start()
        self._task = loop.create_task(self._consume())
        logger.info(f"TG Bot KafkaConsumer запущен для топиков: {self.topics}")

    async def stop(self):
        logger.info("Stopping KafkaBotConsumer...")
        if self._task:
            self._task.cancel()
            try:
                if self._task: await self._task
            except asyncio.CancelledError:
                pass
        if self.consumer:
            await self.consumer.stop()
            logger.info("KafkaBotConsumer stopped.")

    async def _consume(self):
        if not self.consumer: return
        try:
            async for msg in self.consumer:
                logger.info(f"Получено сообщение из топика {msg.topic}: {msg.value}")
                try:
                    if msg.topic == "notification.send":
                        await self._handle_text_message(msg.value)
                    elif msg.topic == "notification.send.document":
                        await self._handle_document_message(msg.value)
                    elif msg.topic == "notification.send.tickets":
                        await self._handle_tickets_list(msg.value)
                    elif msg.topic == "wishlist.view.viewer_success":
                        await self._handle_wishlist_view(msg.value)
                    elif msg.topic == "wishlist.view.owner_success":
                        await self._handle_wishlist_view(msg.value)
                    elif msg.topic in ("wishlist.view.owner_failed",
                                       "wishlist.view.viewer_failed",
                                       "wishlist.view.all_failed",
                                       "wishlist.view.booked_items_failed"):
                        await self.bot.send_message(msg.value["telegram_id"],
                                                    f"❌ Ошибка просмотра: {escape_markdown(msg.value.get('reason', 'N/A'))}")
                    elif msg.topic == "user.user.allowed":
                        user_id = msg.value.get("user_id")
                        if user_id: update_allowed_users(user_id, allow=True)
                    elif msg.topic == "user.user.disallowed":
                        user_id = msg.value.get("user_id")
                        if user_id: update_allowed_users(user_id, allow=False)
                    elif msg.topic == "user.user.list_response":
                        await self._handle_user_list_response(msg.value)
                    elif msg.topic == "wishlist.view.all_success":
                        await self._handle_all_wishlists_view(msg.value)
                    elif msg.topic == "wishlist.item.deleted_booker_notification":
                        if msg.value.get("telegram_id"):
                            await self.bot.send_message(msg.value.get("telegram_id"),
                                                        f"⚠️ Товар '{escape_markdown(msg.value.get('item_name', 'N/A'))}', который ты бронировал, был удален владельцем.")
                    elif msg.topic == "wishlist.item.booked":
                        if msg.value.get("telegram_id"):
                            await self.bot.send_message(msg.value.get("telegram_id"),
                                                        f"✅ Ты забронил '{escape_markdown(msg.value.get('item_name', 'N/A'))}'!")
                    elif msg.topic == "wishlist.item.unbooked":
                        if msg.value.get("telegram_id"):
                            await self.bot.send_message(msg.value.get("telegram_id"),
                                                        f"✅ Ты снял бронь с '{escape_markdown(msg.value.get('item_name', 'N/A'))}'.")
                    elif msg.topic in ("wishlist.item.add_failed", "wishlist.item.book_failed",
                                       "wishlist.item.unbook_failed", "wishlist.item.delete_failed"):
                        if msg.value.get("telegram_id"):
                            await self.bot.send_message(msg.value.get("telegram_id"),
                                                        f"❌ Ошибка: {escape_markdown(msg.value.get('reason', 'N/A'))}")
                    elif msg.topic == "wishlist.item.parse_failed":
                        await self._handle_wishlist_parse_failed(msg.value)
                except Exception as e:
                    logger.error(f"Ошибка обработки сообщения из топика {msg.topic}: {e}", exc_info=True)
        except asyncio.CancelledError:
            logger.info("Задача консумера отменена.")
        except Exception as e:
            logger.error(f"Kafka consumer error: {e}", exc_info=True)
        finally:
            logger.info("Цикл консумера завершен.")

    async def _handle_wishlist_parse_failed(self, value: dict):
        telegram_id = value.get("telegram_id")
        source_url = value.get("source_url", "")
        if not telegram_id:
            return
        logger.warning(f"Parse failed for user {telegram_id}, url {source_url}")
        ctx = FSMContext(self.storage, key=StorageKey(bot_id=self.bot.id, user_id=telegram_id, chat_id=telegram_id))
        prefilled_name = f"Примерно как по ссылке {source_url}"
        await ctx.set_state(WishlistAddManual.waiting_for_cost)
        await ctx.update_data(manual_add_data={
            "telegram_id": telegram_id, "name": prefilled_name, "item_url": source_url
        })
        await self.bot.send_message(telegram_id,
                                    f"Не смог спарсить инфу по ссылке. 😥\n"
                                    f"Давай добавим руками. Я заполнил название:\n{prefilled_name}\n\n"
                                    f"Введи примерную цену (или 'пропустить'):", parse_mode=None)

    async def _handle_all_wishlists_view(self, value: dict):
        requester_id = value.get("telegram_id")
        owner_ids = value.get("owner_user_ids", [])
        logger.info(f"_handle_all_wishlists_view called for {requester_id}. Received owner_ids: {owner_ids}")

        if not requester_id:
            logger.warning(f"No telegram_id in all_wishlists_view payload: {value}")
            return

        ctx = FSMContext(self.storage, key=StorageKey(bot_id=self.bot.id, user_id=requester_id, chat_id=requester_id))

        if not owner_ids:
            logger.info(f"No owner IDs found for {requester_id}. Sending message and clearing state.")
            await self.bot.send_message(requester_id, "Пока никто не создал вишлист.")
            await ctx.clear()
            return

        builder = InlineKeyboardBuilder()
        owner_names = []
        processed_ids = set()

        for owner_id in owner_ids:
            try:
                logger.debug(f"Attempting bot.get_chat for owner_id {owner_id}")
                chat = await self.bot.get_chat(owner_id)
                name = chat.username or chat.full_name
                if name:
                    logger.info(f"Got name '{name}' for owner_id {owner_id}")
                    owner_names.append(name)
                    builder.button(text=name, callback_data=f"all_wishlists_select:{name}")
                    processed_ids.add(owner_id)
                else:
                    logger.warning(f"Could not get username or full_name for user {owner_id}")
                    builder.button(text=f"ID: {owner_id}",
                                   callback_data=f"all_wishlists_select_id:{owner_id}")  # Добавляем кнопку с ID
                    processed_ids.add(owner_id)

            except Exception as e:
                logger.warning(f"Could not fetch info for user {owner_id}: {e}")
                builder.button(text=f"ID: {owner_id} (ошибка)",
                               callback_data=f"all_wishlists_select_id:{owner_id}")  # Помечаем ошибку
                processed_ids.add(owner_id)

        logger.info(f"Collected owner names/IDs: {len(processed_ids)}")
        if not processed_ids:
            logger.info(
                f"No valid owner names or IDs found (excluding self). Sending message and clearing state for {requester_id}.")
            await self.bot.send_message(requester_id, "Некого смотреть (кроме себя).")
            await ctx.clear()
            return

        builder.adjust(2)
        builder.row(InlineKeyboardButton(text="❌ Закрыть", callback_data="all_wishlists_close"))

        await ctx.set_state(AllWishlistsBrowser.choosing_owner)
        logger.info(f"Setting state to choosing_owner for {requester_id}. Sending keyboard.")
        await self.bot.send_message(requester_id, "Выбери, чей вишлист посмотреть:", reply_markup=builder.as_markup())

    async def _handle_text_message(self, value: dict):
        chat_id = value.get("chat_id")
        text = value.get("text")
        if chat_id and text:
            try:
                await self.bot.send_message(chat_id=chat_id, text=text, parse_mode="MarkdownV2")
            except TelegramBadRequest as e:
                logger.error(f"Failed to send MarkdownV2 message: {e}. Text: {text}")
                try:
                    await self.bot.send_message(chat_id=chat_id, text=value.get("text"))
                except Exception as plain_e:
                    logger.error(f"Failed to send even plain text message: {plain_e}")
            except Exception as e:
                logger.error(f"Failed to send text message: {e}")

    async def _handle_document_message(self, value: dict):
        chat_id = value.get("chat_id")
        storage_key = value.get("storage_key")
        caption = value.get("caption")
        filename = value.get("filename", "document.pdf")
        if not all([chat_id, storage_key]): return

        file_bytes = storage_service.download_file_as_bytes(storage_key)
        if not file_bytes:
            await self.bot.send_message(chat_id, "Не удалось загрузить вложение.")
            return

        document = BufferedInputFile(file_bytes, filename=filename)
        try:
            await self.bot.send_document(chat_id, document, caption=caption, parse_mode="MarkdownV2")
        except TelegramBadRequest as e:
            logger.error(f"Failed to send document with MarkdownV2 caption: {e}. Caption: {caption}")
            try:
                await self.bot.send_document(chat_id, document, caption=value.get("caption"))
            except Exception as plain_e:
                logger.error(f"Failed to send document even with plain text caption: {plain_e}")
        except Exception as e:
            logger.error(f"Failed to send document: {e}")

    async def _handle_tickets_list(self, value: dict):
        chat_id = value.get("telegram_id")
        tickets = value.get("tickets")
        if not chat_id or not isinstance(tickets, list): return

        if not tickets:
            await self.bot.send_message(chat_id, "У вас нет предстоящих поездок.")
            return

        await self.bot.send_message(chat_id, f"Найдены билеты ({len(tickets)} шт.):")
        for ticket in tickets:
            builder = InlineKeyboardBuilder()
            builder.button(text="📄 Скачать PDF", callback_data=f"download_ticket:{ticket['ticket_id']}")
            builder.button(text="🗑️ Удалить", callback_data=f"delete_ticket:{ticket['ticket_id']}")

            def format_dt(dt_str):
                if not dt_str: return "н/д"
                try:
                    dt_str = dt_str.replace('Z', '+00:00')
                    if '.' in dt_str.split('+')[0]:
                        dt_obj = datetime.fromisoformat(dt_str)
                    else:
                        parts = dt_str.split('+')
                        dt_part = parts[0]
                        tz_part = parts[1] if len(parts) > 1 else None
                        dt_part += ".000000"
                        full_dt_str = dt_part + ('+' + tz_part if tz_part else '')
                        dt_obj = datetime.fromisoformat(full_dt_str)
                    return dt_obj.strftime('%d.%m.%Y в %H:%M')
                except (ValueError, TypeError) as e:
                    logger.warning(f"Could not parse date string '{dt_str}': {e}")
                    return "н/д"

            text = (
                f"*{ticket['title']}*\n\n"
                f"Пассажир: *{ticket.get('passenger_name') or 'н/д'}*\n"
                f"Поезд: *{ticket.get('train_number') or 'н/д'}* | "
                f"Вагон: *{ticket.get('wagon_number') or 'н/д'}* | "
                f"Место: *{ticket.get('seat_number') or 'н/д'}*\n\n"
                f"📍 *Отправление:* {ticket.get('departure_station') or 'н/д'}\n"
                f"   {format_dt(ticket.get('departure_datetime'))}\n"
                f"🏁 *Прибытие:* {ticket.get('arrival_station') or 'н/д'}\n"
                f"   {format_dt(ticket.get('arrival_datetime'))}"
            )
            try:
                await self.bot.send_message(chat_id, text, reply_markup=builder.as_markup(), parse_mode="Markdown")
            except Exception as e:
                logger.error(f"Failed to send ticket info: {e}")

    async def _handle_wishlist_view(self, value: dict):
        telegram_id = value.get("telegram_id")
        owner_user_id = value.get("owner_user_id")
        items = value.get("items", [])
        logger.info(f"Got wishlist for {telegram_id}, owner {owner_user_id}")

        if not telegram_id or owner_user_id is None:
            logger.warning(f"Invalid wishlist payload: {value}")
            return

        ctx = FSMContext(self.storage, key=StorageKey(bot_id=self.bot.id, user_id=telegram_id, chat_id=telegram_id))
        if not items:
            await self.bot.send_message(telegram_id, "Этот саботажник ничего не добавил.")
            await ctx.clear()
            return

        await ctx.set_state(WishlistBrowser.browsing)
        await ctx.set_data({
            "items": items,
            "current_index": 0,
            "owner_user_id": owner_user_id
        })

        try:
            text, markup = await build_wishlist_page(ctx, telegram_id)
        except Exception as build_e:
            logger.error(f"Error building wishlist page: {build_e}", exc_info=True)
            await self.bot.send_message(telegram_id, "Ошибка при подготовке вишлиста для отображения.")
            return

        try:
            if markup:
                await self.bot.send_message(telegram_id, text, reply_markup=markup, parse_mode=None,
                                            disable_web_page_preview=False)
            else:
                await self.bot.send_message(telegram_id, text, parse_mode=None, disable_web_page_preview=False)
        except Exception as e:
            logger.error(f"Failed to send wishlist view message (plain text): {e}. Text: {text}")
            await self.bot.send_message(telegram_id, "Произошла ошибка при отображении вишлиста.")

    async def _handle_user_list_response(self, value: dict):
        admin_id = value.get("admin_id")
        users = value.get("users", [])

        if not admin_id:
            logger.warning(f"No admin_id in user list: {value}")
            return

        ctx = FSMContext(self.storage, key=StorageKey(bot_id=self.bot.id, user_id=admin_id, chat_id=admin_id))
        if not users:
            await self.bot.send_message(admin_id, "Нет юзеров для удаления.")
            return

        builder = InlineKeyboardBuilder()
        for user in users:
            user_id = user.get("id")
            username = user.get("username")
            label = f"@{username}" if username else f"ID: {user_id}"
            builder.row(InlineKeyboardButton(text=f"🗑️ {label}", callback_data=f"remove_user_confirm:{user_id}"))
        builder.row(InlineKeyboardButton(text="❌ Отмена", callback_data="remove_user_cancel"))
        await self.bot.send_message(admin_id, "Выбери юзера для удаления:", reply_markup=builder.as_markup())
        await ctx.set_state(UserRemoval.choosing_user)