from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.api.v1.routers import router
from app.core.http_client import http_client

@asynccontextmanager
async def lifespan(app: FastAPI):
    await http_client.start()
    yield
    await http_client.stop()

app = FastAPI(title="Telegram Bot Service", lifespan=lifespan)

app.include_router(router, prefix="/api/v1")