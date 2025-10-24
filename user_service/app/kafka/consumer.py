import asyncio
import json
import logging
from aiokafka import AIOKafkaConsumer
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.db import AsyncSessionLocal
from app.core.config import settings
from app.schemas.user_schemas import UserCreate
from app.services.user_service import user_service
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

                db: AsyncSession = AsyncSessionLocal()
                try:
                    if msg.topic == "user.user.create":
                        user_data = UserCreate(**msg.value)
                        await user_service.create_or_update_user(db, user_data)
                    elif msg.topic == "user.user.allow_request":
                        await self.handle_allow_request(db, msg.value)
                    elif msg.topic == "user.user.disallow_request":
                        await self.handle_disallow_request(db, msg.value)
                    elif msg.topic == "user.user.list_request":
                        await self.handle_list_request(db, msg.value)
                    await db.commit()
                except Exception as e:
                    logger.error(f"Error processing {msg.topic}: {e}", exc_info=True)
                    await db.rollback()
                finally:
                    await db.close()
        except asyncio.CancelledError:
            logger.info("Consumer task cancelled.")
        except Exception as e:
            logger.error(f"Kafka consumer error: {e}", exc_info=True)
        finally:
            logger.info("Consumer loop finished.")

    async def handle_allow_request(self, db: AsyncSession, value: dict):
        admin_id = value.get("admin_id")
        target_user_id = value.get("target_user_id")

        if not admin_id or not target_user_id:
            logger.warning(f"Invalid allow_request: {value}")
            return
        if admin_id not in settings.ADMIN_TELEGRAM_IDS:
            logger.warning(f"{admin_id} not admin, tried to allow.")
            await kafka_producer.send("notification.send", {"telegram_id": admin_id, "message": "❌ Нет прав."})
            return

        try:
            allowed_user = await user_service.get_or_create_user_by_id(db, target_user_id)
            await kafka_producer.send("user.user.allowed", {"user_id": allowed_user.telegram_id})
            await kafka_producer.send("notification.send", {"telegram_id": admin_id,
                                                            "message": f"✅ Юзер {allowed_user.telegram_id} добавлен."})
        except Exception as e:
            logger.error(f"Failed to allow {target_user_id}: {e}", exc_info=True)
            await kafka_producer.send("notification.send",
                                      {"telegram_id": admin_id, "message": f"❌ Ошибка добавления {target_user_id}."})

    async def handle_disallow_request(self, db: AsyncSession, value: dict):
        admin_id = value.get("admin_id")
        target_user_id = value.get("target_user_id")

        if not admin_id or not target_user_id:
            logger.warning(f"Invalid disallow_request: {value}")
            return
        if admin_id not in settings.ADMIN_TELEGRAM_IDS:
            logger.warning(f"{admin_id} not admin, tried to disallow.")
            await kafka_producer.send("notification.send", {"telegram_id": admin_id, "message": "❌ Нет прав."})
            return
        if target_user_id in settings.ADMIN_TELEGRAM_IDS:
            logger.warning(f"Admin {admin_id} tried to remove admin {target_user_id}. Denied.")
            await kafka_producer.send("notification.send", {"telegram_id": admin_id,
                                                            "message": f"❌ Нельзя удалить другого админа ({target_user_id})."})
            return

        try:
            deleted = await user_service.delete_user_by_id(db, target_user_id)
            if deleted:
                await kafka_producer.send("user.user.disallowed", {"user_id": target_user_id})
                await kafka_producer.send("notification.send",
                                          {"telegram_id": admin_id, "message": f"✅ Юзер {target_user_id} удален."})
            else:
                await kafka_producer.send("notification.send",
                                          {"telegram_id": admin_id, "message": f"⚠️ Юзер {target_user_id} не найден."})
        except Exception as e:
            logger.error(f"Failed to disallow {target_user_id}: {e}", exc_info=True)
            await kafka_producer.send("notification.send",
                                      {"telegram_id": admin_id, "message": f"❌ Ошибка удаления {target_user_id}."})

    async def handle_list_request(self, db: AsyncSession, value: dict):
        admin_id = value.get("admin_id")
        if not admin_id:
            logger.warning(f"Invalid list_request: {value}")
            return
        if admin_id not in settings.ADMIN_TELEGRAM_IDS:
            logger.warning(f"{admin_id} not admin, tried to list users.")
            return

        try:
            users = await user_service.get_all_users_except_admins(db, settings.ADMIN_TELEGRAM_IDS)
            user_list = [{"id": u.telegram_id, "username": u.username} for u in users]
            await kafka_producer.send("user.user.list_response", {"admin_id": admin_id, "users": user_list})
        except Exception as e:
            logger.error(f"Failed to list users for {admin_id}: {e}", exc_info=True)
            await kafka_producer.send("notification.send",
                                      {"telegram_id": admin_id, "message": "❌ Ошибка получения списка юзеров."})


kafka_consumer = KafkaConsumer(
    "user.user.create",
    "user.user.allow_request",
    "user.user.disallow_request",
    "user.user.list_request"
)