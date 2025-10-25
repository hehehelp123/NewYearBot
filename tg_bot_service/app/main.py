import logging
from fastapi import FastAPI
from app.bot_startup import lifespan

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Telegram Bot Service",
    lifespan=lifespan
)

@app.get("/")
def read_root():
    return {"service": "Telegram Bot Service", "status": "ok"}