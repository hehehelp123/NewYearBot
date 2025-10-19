import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from aiokafka.errors import KafkaConnectionError

from app.api.v1.routers import router
from app.kafka.producer import kafka_producer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Управляет жизненным циклом приложения с надежным запуском Kafka.
    """
    logging.info("Application lifespan startup...")

    retry_interval = 2
    max_retries = 15
    for i in range(max_retries):
        try:
            logging.info(f"Attempt {i + 1}/{max_retries} to connect to Kafka...")
            await kafka_producer.start()
            logging.info("✅ Kafka producer started successfully.")
            break
        except KafkaConnectionError as e:
            if i + 1 == max_retries:
                logging.error(f"❌ Could not connect to Kafka after all retries. Error: {e}. Exiting.")
                raise
            logging.warning(
                f"Kafka not ready yet ({e}). Retrying in {retry_interval:.1f} seconds..."
            )
            await asyncio.sleep(retry_interval)
            retry_interval *= 1.5

    yield

    logging.info("Application lifespan shutdown...")
    await kafka_producer.stop()
    logging.info("Kafka producer stopped.")


app = FastAPI(
    title="Wishlist Service",
    lifespan=lifespan
)

app.include_router(router, prefix="/api/v1")