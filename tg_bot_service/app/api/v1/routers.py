from fastapi import APIRouter
from app.schemas.bot_schemas import StartCommand
from app.services.bot_service import bot_service

router = APIRouter()

@router.get("/")
def read_root():
    return {"service": "Telegram Bot Service", "status": "ok"}

@router.post("/commands/start")
async def handle_start(payload: StartCommand):
    # В реальном боте здесь будет логика обработки вебхука от Telegram
    # Мы симулируем получение команды /start
    return await bot_service.handle_start_command(payload)