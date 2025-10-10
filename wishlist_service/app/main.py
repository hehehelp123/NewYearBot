from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.api.v1.routers import router
from app.kafka.producer import kafka_producer

@asynccontextmanager
async def lifespan(app: FastAPI):
    await kafka_producer.start()
    yield
    await kafka_producer.stop()

app = FastAPI(title="Wishlist Service", lifespan=lifespan)

app.include_router(router, prefix="/api/v1")