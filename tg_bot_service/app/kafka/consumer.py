import asyncio
import json
import logging
from aiogram import Bot
from aiokafka import AIOKafkaConsumer
from app.core.config import settings

logger = logging.getLogger(__name__)

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
        if self._task:
            self._task.cancel()
        if self.consumer:
            await self.consumer.stop()
        logger.info("TG Bot KafkaConsumer остановлен.")

    async def _consume(self):
        if not self.consumer: return
        try:
            async for msg in self.consumer:
                logger.info(f"Получено сообщение для отправки: {msg.value}")
                chat_id = msg.value.get("chat_id")
                text = msg.value.get("text")
                if chat_id and text:
                    await self.bot.send_message(chat_id=chat_id, text=text)
        except asyncio.CancelledError:
            logger.info("Задача консумера отменена.")
        finally:
            logger.info("Цикл консумера завершен.")