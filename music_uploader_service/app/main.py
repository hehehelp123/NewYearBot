from contextlib import asynccontextmanager
import logging
from fastapi import FastAPI
from app.api.v1.routers import router
from app.kafka.producer import kafka_producer
from app.services.yandex_music_service import yandex_music_sync_service

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Запуск music_uploader_service...")
    await kafka_producer.start()
    try:
        await yandex_music_sync_service.start()
    except Exception as e:
        logger.error(f"Критическая ошибка при инициализации Yandex Music Service: {e}")
    yield
    await kafka_producer.stop()
    logger.info("Остановка music_uploader_service.")

app = FastAPI(title="Music Uploader Service", lifespan=lifespan)
app.include_router(router, prefix="/api/v1")