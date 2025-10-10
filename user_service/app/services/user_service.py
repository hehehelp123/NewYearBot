from sqlalchemy.ext.asyncio import AsyncSession
from app.models.user_models import User
from app.schemas.user_schemas import UserCreate

class UserService:
    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session

    async def create_user(self, user: UserCreate) -> User:
        db_user = User(**user.model_dump())
        self.db_session.add(db_user)
        await self.db_session.commit()
        await self.db_session.refresh(db_user)
        return db_user