import asyncio
import json
import logging
from aiokafka import AIOKafkaConsumer
from sqlalchemy.orm import Session
from app.core.db import SessionLocal
from app.core.config import settings
from app.services.user_service import user_service, UserCreateRequest
from app.kafka.producer import kafka_producer

logger = logging.getLogger(__name__)

class KafkaConsumer:
    def __init__(self, *topics: str):
        self.topics = topics
        self.consumer: AIOKafkaConsumer | None = None
        self._task = None

    async def start(self):
        logger.info(f"Starting KafkaConsumer for topics: {self.topics}")
        loop = asyncio.get_event_loop()
        self.consumer = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            loop=loop,
            group_id="user_service_group",
            auto_offset_reset='earliest',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        await self.consumer.start()
        self._task = loop.create_task(self._consume())
        logger.info("KafkaConsumer started.")

    async def stop(self):
        logger.info("Stopping KafkaConsumer...")
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self.consumer:
            await self.consumer.stop()
        logger.info("KafkaConsumer stopped.")

    async def _consume(self):
        try:
            async for msg in self.consumer:
                logger.info(f"Consumed from {msg.topic}: key={msg.key} value={msg.value}")
                db: Session = SessionLocal()
                try:
                    if msg.topic == "user.user.create":
                        user_data = UserCreateRequest(**msg.value)
                        await user_service.create_or_update_user(db, user_data)
                    elif msg.topic == "user.user.allow_request":
                        await self.handle_allow_request(db, msg.value)
                except Exception as e:
                    logger.error(f"Error processing message from {msg.topic}: {e}", exc_info=True)
                finally:
                    db.close()
        except asyncio.CancelledError:
            logger.info("Consumer task cancelled.")
        except Exception as e:
            logger.error(f"Kafka consumer error: {e}", exc_info=True)
        finally:
            logger.info("Consumer loop finished.")

    async def handle_allow_request(self, db: Session, value: dict):
        admin_id = value.get("admin_id")
        target_user_id = value.get("target_user_id")

        if not admin_id or not target_user_id:
            logger.warning(f"Invalid allow_request payload: {value}")
            return

        if admin_id not in settings.ADMIN_TELEGRAM_IDS:
            logger.warning(f"User {admin_id} attempted to allow user, but is not an admin.")
            await kafka_producer.send("notification.send", {
                "telegram_id": admin_id,
                "message": f"❌ Ошибка: У вас нет прав добавлять пользователей."
            })
            return

        try:
            allowed_user = await user_service.allow_user_by_id(db, target_user_id)
            await kafka_producer.send("user.user.allowed", {"user_id": allowed_user.telegram_id})
            await kafka_producer.send("notification.send", {
                "telegram_id": admin_id,
                "message": f"✅ Пользователь с ID {allowed_user.telegram_id} успешно добавлен/уже был в базе."
            })
        except Exception as e:
             logger.error(f"Failed to allow user {target_user_id}: {e}", exc_info=True)
             await kafka_producer.send("notification.send", {
                "telegram_id": admin_id,
                "message": f"❌ Произошла ошибка при добавлении пользователя {target_user_id}."
            })


kafka_consumer = KafkaConsumer("user.user.create", "user.user.allow_request")