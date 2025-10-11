from fastapi import APIRouter, Request
from app.services.bot_service import bot_service
from app.schemas.bot_schemas import HealthCheckResponse

router = APIRouter()

@router.get("/health", response_model=HealthCheckResponse)
async def health_check():
    return await bot_service.health_check()

@router.get("/menu")
async def get_menu_from_orchestrator(request: Request):
    return request.app.state.menu_tree

@router.get("/")
def read_root():
    return {"service": "Telegram Bot Service", "status": "ok"}