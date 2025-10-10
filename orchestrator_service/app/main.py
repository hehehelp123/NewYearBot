from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.api.v1.routers import router
from app.kafka.producer import kafka_producer
from app.services.orchestration_service import orchestration_service
from app.services.menu_service import menu_service

@asynccontextmanager
async def lifespan(app: FastAPI):
    await kafka_producer.start()
    await menu_service.build_menu_tree()
    app.state.menu_tree = menu_service.get_menu_tree()
    yield
    await kafka_producer.stop()
    await orchestration_service.close()
    await menu_service.close()

app = FastAPI(title="Orchestrator Service", lifespan=lifespan)

app.include_router(router, prefix="/api/v1")