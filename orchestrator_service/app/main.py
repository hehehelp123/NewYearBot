from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.api.v1.routers import router
from app.kafka.producer import kafka_producer
from app.services.menu_service import menu_service

@asynccontextmanager
async def lifespan(app: FastAPI):
    await kafka_producer.start()
    menu_tree, command_map = await menu_service.build_menu_tree()
    app.state.menu_tree = menu_tree
    app.state.command_map = command_map
    yield
    await kafka_producer.stop()
    await menu_service.close()

app = FastAPI(title="Orchestrator Service", lifespan=lifespan)
app.include_router(router, prefix="/api/v1")