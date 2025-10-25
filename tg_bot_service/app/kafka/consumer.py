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
from app.bot.bot_app import build_wishlist_page, WishlistBrowser, UserRemoval, AllWishlistsBrowser
from app.middlewares.access_middleware import update_allowed_users

from app.core.config import settings
from app.services.storage_service import storage_service
from app.bot.bot_app import build_wishlist_page, WishlistBrowser, UserRemoval
from app.middlewares.access_middleware import update_allowed_users

logger = logging.getLogger(__name__)


def escape_markdown(text: str) -> str:
    if not isinstance(text, str):
        return ""
    escape_chars = r"[_*\[\]()~`>#\+\-=|{}.!]"
    return re.sub(f"({escape_chars})", r"\\\1", text)


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
                    elif msg.topic in "wishlist.view.viewer_success":
                        await self._handle_wishlist_view(msg.value)
                    elif msg.topic in ("wishlist.view.owner_failed", "wishlist.view.viewer_failed"):
                        await self.bot.send_message(msg.value["telegram_id"], msg.value.get("error", "Failed wishlist."))
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
    
                except Exception as e:
                    logger.error(f"Ошибка обработки сообщения: {e}", exc_info=True)
        except asyncio.CancelledError:
            logger.info("Задача консумера отменена.")
        except Exception as e:
            logger.error(f"Kafka consumer error: {e}", exc_info=True)
        finally:
            logger.info("Цикл консумера завершен.")

    async def _handle_all_wishlists_view(self, value: dict):
            requester_id = value.get("telegram_id")
            owner_ids = value.get("owner_user_ids", [])
            
            if not requester_id:
                logger.warning(f"No telegram_id in all_wishlists_view payload: {value}")
                return
                
            ctx = FSMContext(self.storage, key=StorageKey(bot_id=self.bot.id, user_id=requester_id, chat_id=requester_id))
            
            if not owner_ids:
                await self.bot.send_message(requester_id, "Пока никто не создал вишлист.")
                await ctx.clear()
                return
            
            builder = InlineKeyboardBuilder()
            owner_names = []
            
            for owner_id in owner_ids:
                if owner_id == requester_id:
                    continue
                try:
                    chat = await self.bot.get_chat(owner_id)
                    name = chat.username or chat.full_name
                    if name:
                        owner_names.append(name)
                        builder.button(text=name, callback_data=f"all_wishlists_select:{name}")
                except Exception as e:
                    logger.warning(f"Could not fetch info for user {owner_id}: {e}")
                    
            if not owner_names:
                await self.bot.send_message(requester_id, "Некого смотреть (кроме себя).")
                await ctx.clear()
                return
                
            builder.adjust(2)
            builder.row(InlineKeyboardButton(text="❌ Закрыть", callback_data="all_wishlists_close"))
            
            await ctx.set_state(AllWishlistsBrowser.choosing_owner)
            await self.bot.send_message(requester_id, "Выбери, чей вишлист посмотреть:", reply_markup=builder.as_markup())
    
    async def _handle_text_message(self, value: dict):
        chat_id = value.get("chat_id")
        text = value.get("text")
        if chat_id and text:
            await self.bot.send_message(chat_id=chat_id, text=text, parse_mode="MarkdownV2")

    async def _handle_document_message(self, value: dict):
        chat_id = value.get("chat_id")
        storage_key = value.get("storage_key")
        caption = value.get("caption")
        filename = value.get("filename", "document.pdf")
        if not all([chat_id, storage_key]): return

        file_bytes = storage_service.download_file_as_bytes(storage_key)
        if not file_bytes:
            await self.bot.send_message(chat_id, "Не удалось загрузить вложение\\.")
            return

        document = BufferedInputFile(file_bytes, filename=filename)
        await self.bot.send_document(chat_id, document, caption=caption, parse_mode="MarkdownV2")

    async def _handle_tickets_list(self, value: dict):
        chat_id = value.get("telegram_id")
        tickets = value.get("tickets")
        if not chat_id or not isinstance(tickets, list): return

        if not tickets:
            await self.bot.send_message(chat_id, "У вас нет предстоящих поездок\\.")
            return

        await self.bot.send_message(chat_id, f"Найдены билеты ({len(tickets)} шт\\.):")
        for ticket in tickets:
            builder = InlineKeyboardBuilder()
            builder.button(text="📄 Скачать PDF", callback_data=f"download_ticket:{ticket['ticket_id']}")
            builder.button(text="🗑️ Удалить", callback_data=f"delete_ticket:{ticket['ticket_id']}")

            def format_dt(dt_str):
                return escape_markdown(datetime.fromisoformat(dt_str).strftime('%d.%m.%Y в %H:%M')) if dt_str else "н/д"

            text = (
                f"*{escape_markdown(ticket['title'])}*\n\n"
                f"Пассажир: *{escape_markdown(ticket.get('passenger_name') or 'н/д')}*\n"
                f"Поезд: *{escape_markdown(ticket.get('train_number') or 'н/д')}* \\| "
                f"Вагон: *{escape_markdown(ticket.get('wagon_number') or 'н/д')}* \\| "
                f"Место: *{escape_markdown(ticket.get('seat_number') or 'н/д')}*\n\n"
                f"📍 *Отправление:* {escape_markdown(ticket.get('departure_station') or 'н/д')}\n"
                f"   {format_dt(ticket.get('departure_datetime'))}\n"
                f"🏁 *Прибытие:* {escape_markdown(ticket.get('arrival_station') or 'н/д')}\n"
                f"   {format_dt(ticket.get('arrival_datetime'))}"
            )
            await self.bot.send_message(chat_id, text, reply_markup=builder.as_markup(), parse_mode="MarkdownV2")

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
        text, markup = await build_wishlist_page(ctx, telegram_id)
        if markup:
            await self.bot.send_message(telegram_id, text, reply_markup=markup, parse_mode="MarkdownV2")
        else:
            await self.bot.send_message(telegram_id, text, parse_mode="MarkdownV2")

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