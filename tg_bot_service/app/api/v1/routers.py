from fastapi import APIRouter
from app.schemas.bot_schemas import StartCommand
from app.services.bot_service import bot_service

router = APIRouter()

@router.get("/menu")
async def get_menu_from_orchestrator():
    return await bot_service.get_menu()


@router.get("/")
def read_root():
    return {"service": "Telegram Bot Service", "status": "ok"}

@router.post("/commands/start")
async def handle_start(payload: StartCommand):
    return await bot_service.handle_start_command(payload)