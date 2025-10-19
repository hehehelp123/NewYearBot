from contextlib import asynccontextmanager
from fastapi import FastAPI
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
    "wishlist.view.get_owner",
    "wishlist.view.get_viewer"
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Запуск wishlist_service...")
    await kafka_producer.start()
    await consumer.start()
    logger.info("Scheduling background task for cookie export.")
    asyncio.create_task(selenium_downloader.run_cookie_export_background())

    yield

    await kafka_producer.stop()
    await consumer.stop()
    logger.info("Остановка wishlist_service.")

app = FastAPI(title="Wishlist Service", lifespan=lifespan)  