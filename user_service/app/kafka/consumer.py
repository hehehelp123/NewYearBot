import asyncio
import json
import logging
from aiokafka import AIOKafkaConsumer
from app.core.config import settings
from app.core.db import AsyncSessionLocal
from app.services.user_service import UserService
from app.schemas.user_schemas import UserCreate
from sqlalchemy.exc import IntegrityError

logger = logging.getLogger(__name__)


async def handle_create_user_event(event_data: dict):
    async with AsyncSessionLocal() as session:
        user_service = UserService(session)
        try:
            user_create = UserCreate(**event_data)
            await user_service.create_user(user_create)
            logger.info(f"User created from Kafka event: {user_create.telegram_id}")
        except IntegrityError:
            logger.warning(f"User with telegram_id {event_data.get('telegram_id')} already exists. Skipping creation.")
        except Exception as e:
            logger.error(f"Error processing create_user event: {e}")


class KafkaConsumer:
    def __init__(self, *topics: str):
        self.topics = topics
        self.consumer: AIOKafkaConsumer | None = None
        self._task = None

    async def start(self):
        if not self.consumer:
            self.consumer = AIOKafkaConsumer(
                *self.topics,
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                group_id="user_service_group",
                auto_offset_reset='earliest',
                value_deserializer=lambda v: json.loads(v.decode('utf-8'))
            )

        logger.info(f"Starting KafkaConsumer for topics: {self.topics}")
        await self.consumer.start()
        self._task = asyncio.create_task(self._consume())

    async def stop(self):
        logger.info("Stopping KafkaConsumer...")
        if self._task:
            self._task.cancel()
        if self.consumer:
            await self.consumer.stop()
        logger.info("KafkaConsumer stopped.")

    async def _consume(self):
        if not self.consumer:
            return

        try:
            async for msg in self.consumer:
                logger.info(f"Consumed from {msg.topic}: value={msg.value}")
                if msg.topic == "user.user.create":
                    await handle_create_user_event(msg.value)
        except asyncio.CancelledError:
            logger.info("Consumer task cancelled.")
        finally:
            logger.info("Consumer loop finished.")