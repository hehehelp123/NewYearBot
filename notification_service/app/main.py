from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.api.v1.routers import router
from app.kafka.consumer import KafkaConsumer

# Здесь мы указываем, какие топики будет слушать этот сервис
consumer = KafkaConsumer("user_created", "ticket_created", "wishlist_item_added")

@asynccontextmanager
async def lifespan(app: FastAPI):
    await consumer.start()
    yield
    await consumer.stop()

app = FastAPI(title="Notification Service", lifespan=lifespan)

app.include_router(router, prefix="/api/v1")