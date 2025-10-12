import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.api.v1.routers import router
from app.kafka.producer import kafka_producer
from app.services.sync_service import music_sync_service

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Запуск music_uploader_service...")
    await kafka_producer.start()
    await music_sync_service.start()

    yield

    await kafka_producer.stop()
    music_sync_service.stop()
    logger.info("Остановка music_uploader_service.")


app = FastAPI(title="Music Uploader Service", lifespan=lifespan)
app.include_router(router, prefix="/api/v1")