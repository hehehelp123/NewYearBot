from fastapi import APIRouter
from app.services.bot_service import bot_service
from app.schemas.bot_schemas import HealthCheckResponse

router = APIRouter()

@router.get("/health", response_model=HealthCheckResponse)
async def health_check():
    return await bot_service.health_check()