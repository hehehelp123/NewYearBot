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

from app.core.config import settings
from app.services.storage_service import storage_service
from app.bot.bot_app import build_wishlist_page, WishlistBrowser, UserRemoval
from app.middlewares.access_middleware import update_allowed_users

logger = logging.getLogger(__name__)

def escape_markdown(text: str) -> str:
    if not isinstance(text, str): return ""
    escape_chars = r"[_*\[\]()~`>#\+\-=|{}.!]"
    return re.sub(f"({escape_chars})", r"\\\1", text)

def format_dt(dt_str):
    if not dt_str: return "н/д"
    try:
        dt_obj = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        return escape_markdown(dt_obj.strftime("%d.%m.%Y %H:%M"))
    except ValueError:
        return escape_markdown(dt_str)

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
        logger.info("KafkaBotConsumer started.")

    async def stop(self):
        logger.info("Stopping KafkaBotConsumer...")
        if self._task:
            self._task.cancel()
            try:
                if self._task: await self._task
            except asyncio.CancelledError: pass
        if self.consumer:
            await self.consumer.stop()
            logger.info("KafkaBotConsumer stopped.")

    async def _consume(self):
        if not self.consumer: return
        try:
            async for msg in self.consumer:
                logger.info(f"Consumed {msg.topic}: key={msg.key} value={msg.value}")
                try:
                    if msg.topic == "notification.send":
                        await self.bot.send_message(msg.value["telegram_id"], msg.value["message"])
                    elif msg.topic == "notification.send.document":
                        await self._handle_send_document(msg.value)
                    elif msg.topic == "notification.send.tickets":
                        await self._handle_send_tickets(msg.value)
                    elif msg.topic in ("wishlist.view.owner_success", "wishlist.view.viewer_success"):
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
                except Exception as e:
                    logger.error(f"Error processing {msg.topic}: {e}", exc_info=True)
        except asyncio.CancelledError:
            logger.info("Consumer task cancelled.")
        except Exception as e:
            logger.error(f"Kafka consumer error: {e}", exc_info=True)
        finally:
            logger.info("Consumer loop finished.")

    async def _handle_send_document(self, value: dict):
        telegram_id=value.get("telegram_id")
        object_name=value.get("object_name")
        caption=value.get("caption")
        filename=value.get("filename", object_name.split('/')[-1] if object_name else "doc.pdf")

        if not telegram_id or not object_name:
            logger.error(f"Invalid doc payload: {value}")
            return

        file_bytes = storage_service.download_file_as_bytes(object_name)
        if file_bytes:
            input_file = BufferedInputFile(file_bytes, filename=filename)
            await self.bot.send_document(telegram_id, input_file, caption=caption)
        else:
            await self.bot.send_message(telegram_id, f"Не скачал {filename}.")

    async def _handle_send_tickets(self, value: dict):
        chat_id=value.get("telegram_id")
        tickets=value.get("tickets", [])

        if not chat_id:
            logger.warning(f"No telegram_id for tickets: {value}")
            return
        if not tickets:
            await self.bot.send_message(chat_id, "Билетов нет.")
            return

        await self.bot.send_message(chat_id, "Твои билеты:")
        for ticket in tickets:
            builder = InlineKeyboardBuilder()
            builder.button(text="📄 Скачать", callback_data=f"download_ticket:{ticket['id']}")
            builder.button(text="🗑️ Удалить", callback_data=f"delete_ticket:{ticket['id']}")
            text = (
                f"*{escape_markdown(ticket.get('title', 'Билет'))}*\n"
                f"{escape_markdown(ticket.get('departure_station') or 'н/д')} \\- {escape_markdown(ticket.get('arrival_station') or 'н/д')}\\n"
                f"   {format_dt(ticket.get('departure_datetime'))} \\-\\> {format_dt(ticket.get('arrival_datetime'))}"
            )
            await self.bot.send_message(chat_id, text, reply_markup=builder.as_markup(), parse_mode="MarkdownV2")

    async def _handle_wishlist_view(self, value: dict):
        telegram_id=value.get("telegram_id")
        owner_user_id=value.get("owner_user_id")
        items=value.get("items", [])
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