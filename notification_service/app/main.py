import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from app.api.v1.routers import router
from app.kafka.producer import kafka_producer
from app.kafka.consumer import KafkaConsumer
from app.core.db import AsyncSessionLocal
from app.services.notification_service import NotificationService

logger = logging.getLogger(__name__)


async def run_scheduler():
    while True:
        try:
            async with AsyncSessionLocal() as session:
                service = NotificationService(session)
                await service.process_scheduled_notifications()
        except Exception as e:
            logger.error(f"Ошибка в планировщике уведомлений: {e}")
        await asyncio.sleep(60)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Запуск notification_service...")
    await kafka_producer.start()

    # Добавляем selenium.upload.success в список прослушиваемых топиков
    consumer = KafkaConsumer(
        "user.user.created",
        "ticket.ticket.created",
        "selenium.upload.success"
    )
    await consumer.start()

    scheduler_task = asyncio.create_task(run_scheduler())

    yield

    scheduler_task.cancel()
    await kafka_producer.stop()
    await consumer.stop()
    logger.info("Остановка notification_service.")


app = FastAPI(title="Notification Service", lifespan=lifespan)
app.include_router(router, prefix="/api/v1")