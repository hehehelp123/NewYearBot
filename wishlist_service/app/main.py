import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from aiokafka.errors import KafkaConnectionError

from app.api.v1.routers import router
from app.kafka.producer import kafka_producer
from app.kafka.consumer import KafkaConsumer
import logging
from app.services.scraper_service import scraper_service
from app.services.selenium_downloader import selenium_downloader
import asyncio

logging.basicConfig(level = logging.INFO)
logger = logging.getLogger(__name__)

consumer = KafkaConsumer(
    "wishlist.wishlist.add",
    "wishlist.wishlist.create",
    "wishlist.item.book",
    "wishlist.item.unbook",
    "wishlist.view.viewer",
    "wishlist.view.owner",
    "wishlist.item.delete",
    "wishlist.item.add_manual",
    "wishlist.view.all",
    "wishlist.view.booked_items"
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.info("Application lifespan startup...")
    retry_interval = 2
    max_retries = 15
    for i in range(max_retries):
        try:
            logging.info(f"Attempt {i + 1}/{max_retries} to connect to Kafka...")
            await kafka_producer.start()
            await consumer.start()
            logger.info("Scheduling background task for cookie export.")
            asyncio.create_task(selenium_downloader.run_cookie_export_background())
            logging.info("✅ Kafka producer and consumer started successfully.")
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
        except Exception as e:
             logger.error(f"❌ An unexpected error occurred during Kafka startup: {e}. Exiting.", exc_info=True)
             raise

    yield

    logging.info("Application lifespan shutdown...")
    await kafka_producer.stop()
    await consumer.stop()
    logging.info("Kafka producer and consumer stopped.")

app = FastAPI(
    title="Wishlist Service",
    lifespan=lifespan
)

app.include_router(router, prefix="/api/v1")