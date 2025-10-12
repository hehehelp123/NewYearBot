import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.api.v1.routers import router
from app.kafka.producer import kafka_producer
from app.kafka.consumer import KafkaConsumer
from app.services.sync_service import music_sync_service

logger = logging.getLogger(__name__)

consumer = KafkaConsumer("music.sync.start")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Запуск music_uploader_service...")
    await kafka_producer.start()
    await consumer.start()
    try:
        await music_sync_service.start()
    except Exception as e:
        logger.error(f"Критическая ошибка при инициализации Music Sync Service: {e}")

    yield

    await kafka_producer.stop()
    await consumer.stop()
    music_sync_service.stop()
    logger.info("Остановка music_uploader_service.")


app = FastAPI(title="Music Uploader Service", lifespan=lifespan)
app.include_router(router, prefix="/api/v1")