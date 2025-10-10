from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.api.v1.routers import router
from app.kafka.producer import kafka_producer
from app.services.orchestration_service import orchestration_service

@asynccontextmanager
async def lifespan(app: FastAPI):
    await kafka_producer.start()
    yield
    await kafka_producer.stop()
    await orchestration_service.close()

app = FastAPI(title="Orchestrator Service", lifespan=lifespan)

app.include_router(router, prefix="/api/v1")