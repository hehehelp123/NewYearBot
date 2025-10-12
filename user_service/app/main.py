from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.api.v1.routers import router
from app.kafka.producer import kafka_producer
from app.kafka.consumer import KafkaConsumer

consumer = KafkaConsumer("user.user.create")

@asynccontextmanager
async def lifespan(app: FastAPI):
    await kafka_producer.start()
    await consumer.start()
    yield
    await kafka_producer.stop()
    await consumer.stop()

app = FastAPI(title="User Service", lifespan=lifespan)
app.include_router(router, prefix="/api/v1")