import asyncio
import json
import logging
import os
import re
from datetime import datetime
from aiogram import Bot
from aiogram.types import BufferedInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiokafka import AIOKafkaConsumer
from app.core.config import settings
from app.services.storage_service import storage_service

logger = logging.getLogger(__name__)


def escape_markdown(text: str) -> str:
    if not isinstance(text, str):
        return ""
    escape_chars = r"[_*\[\]()~`>#\+\-=|{}.!]"
    return re.sub(f"({escape_chars})", r"\\\1", text)


class KafkaBotConsumer:
    def __init__(self, bot: Bot, *topics: str):
        self.bot = bot
        self.topics = topics
        self.consumer: AIOKafkaConsumer | None = None
        self._task = None

    async def start(self):
        self.consumer = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id="tg_bot_group",
            auto_offset_reset='earliest',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        await self.consumer.start()
        self._task = asyncio.create_task(self._consume())
        logger.info(f"TG Bot KafkaConsumer запущен для топиков: {self.topics}")

    async def stop(self):
        if self._task: self._task.cancel()
        if self.consumer: await self.consumer.stop()
        logger.info("TG Bot KafkaConsumer остановлен.")

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
                except Exception as e:
                    logger.error(f"Ошибка обработки сообщения: {e}", exc_info=True)
        except asyncio.CancelledError:
            logger.info("Задача консумера отменена.")
        finally:
            logger.info("Цикл консумера завершен.")

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
        chat_id = value.get("chat_id")
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
                f"Поезд: *{escape_markdown(ticket.get('train_number') or 'н/д')}* | "
                f"Вагон: *{escape_markdown(ticket.get('wagon_number') or 'н/д')}* | "
                f"Место: *{escape_markdown(ticket.get('seat_number') or 'н/д')}*\n\n"
                f"📍 *Отправление:* {escape_markdown(ticket.get('departure_station') or 'н/д')}\n"
                f"   {format_dt(ticket.get('departure_datetime'))}\n"
                f"🏁 *Прибытие:* {escape_markdown(ticket.get('arrival_station') or 'н/д')}\n"
                f"   {format_dt(ticket.get('arrival_datetime'))}"
            )
            await self.bot.send_message(chat_id, text, reply_markup=builder.as_markup(), parse_mode="MarkdownV2")