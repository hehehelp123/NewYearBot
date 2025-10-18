import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.api.v1.routers import router
from app.kafka.consumer import music_kafka_consumer
from app.services.sync_service import music_sync_service

logging.basicConfig(level=logging.INFO)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.info("Music Uploader Service starting up...")
    await music_sync_service.start()
    await music_kafka_consumer.start()
    yield
    logging.info("Music Uploader Service shutting down...")
    await music_kafka_consumer.stop()
    music_sync_service.stop()

app = FastAPI(
    title="Music Uploader Service",
    description="Service for downloading and uploading music from various sources.",
    lifespan=lifespan
)

app.include_router(router, prefix="/api/v1")