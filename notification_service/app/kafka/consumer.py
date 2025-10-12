import asyncio
import json
import logging
from aiokafka import AIOKafkaConsumer
from app.core.config import settings
from app.core.db import AsyncSessionLocal
from app.services.notification_service import NotificationService

logger = logging.getLogger(__name__)


async def handle_event(event_data: dict, event_type: str):
    async with AsyncSessionLocal() as session:
        notification_service = NotificationService(session)
        await notification_service.create_and_send_notification(event_data, event_type)


class KafkaConsumer:
    def __init__(self, *topics: str):
        self.topics = topics
        self.consumer = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            loop=asyncio.get_event_loop(),
            group_id="notification_group",
            auto_offset_reset='earliest',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        self._task = None

    async def start(self):
        logger.info(f"Starting KafkaConsumer for topics: {self.topics}")
        await self.consumer.start()
        self._task = asyncio.create_task(self._consume())

    async def stop(self):
        logger.info("Stopping KafkaConsumer...")
        if self._task:
            self._task.cancel()
        await self.consumer.stop()
        logger.info("KafkaConsumer stopped.")

    async def _consume(self):
        try:
            async for msg in self.consumer:
                try:
                    logger.info(f"Consumed from {msg.topic}: value={msg.value}")
                    await handle_event(msg.value, msg.topic)
                except Exception as e:
                    logger.error(
                        f"Failed to process message from topic {msg.topic}: {e}",
                        exc_info=True
                    )
                    # В зависимости от стратегии, здесь можно отправить
                    # сообщение в "dead-letter-queue" или просто продолжить
        except asyncio.CancelledError:
            logger.info("Consumer task cancelled.")
        finally:
            logger.info("Consumer loop finished.")