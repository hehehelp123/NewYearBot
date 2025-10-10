from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError
from app.core.db import get_db
from app.schemas.user_schemas import User, UserCreate
from app.services.user_service import UserService

router = APIRouter()

@router.get("/")
def read_root():
    return {"service": "User Service", "status": "ok"}

@router.post("/users/", response_model=User)
async def create_user(user: UserCreate, db: AsyncSession = Depends(get_db)):
    user_service = UserService(db)
    try:
        new_user = await user_service.create_user(user)
        return new_user
    except IntegrityError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="User with this telegram_id already exists."
        )