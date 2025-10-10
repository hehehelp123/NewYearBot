from fastapi import APIRouter
from app.schemas.user_schemas import UserCreateRequest
from app.services.orchestration_service import orchestration_service

router = APIRouter()

@router.get("/")
def read_root():
    return {"service": "Orchestrator Service", "status": "ok"}

@router.post("/users/")
async def register_user(user: UserCreateRequest):
    return await orchestration_service.register_user(user)