import logging
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from aiokafka.errors import KafkaConnectionError

from app.kafka.producer import kafka_producer
from app.kafka.consumer import kafka_consumer
from app.core.config import settings
from app.api.v1 import routers as v1_routers


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Application lifespan startup...")

    kafka_ready = False
    for i in range(settings.KAFKA_CONNECT_RETRIES):
        try:
            await kafka_producer.start()
            logger.info("KafkaProducer started.")
            kafka_ready = True
            break
        except KafkaConnectionError as e:
            logger.warning(f"Kafka not ready yet ({e}). Retrying in {settings.KAFKA_CONNECT_RETRY_DELAY} seconds...")
            await asyncio.sleep(settings.KAFKA_CONNECT_RETRY_DELAY)

    if not kafka_ready:
        logger.error("Kafka connection failed after all retries. Shutting down.")
        raise RuntimeError("Failed to connect to Kafka")

    await kafka_consumer.start()

    logger.info("✅ Kafka producer and consumer started successfully.")
    yield

    await kafka_producer.stop()
    await kafka_consumer.stop()
    logger.info("Application lifespan shutdown...")


app = FastAPI(
    title="Ticket Service",
    description="Manages tickets and parses PDF files",
    version="1.0.0",
    lifespan=lifespan
)

app.include_router(v1_routers.router, prefix="/api/v1")


@app.get("/health")
async def health_check():
    return {"status": "ok"}