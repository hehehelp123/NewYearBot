import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete
from app.models.user_models import User
from app.schemas.user_schemas import UserCreate
from typing import List

logger = logging.getLogger(__name__)


class UserService:
    async def create_or_update_user(self, db: AsyncSession, user_data: UserCreate) -> User:
        stmt = select(User).where(User.telegram_id == user_data.telegram_id)
        result = await db.scalars(stmt)
        db_user = result.first()

        if db_user:
            logger.info(f"User {user_data.telegram_id} exists. Updating username.")
            if user_data.username and db_user.username != user_data.username:
                db_user.username = user_data.username
                await db.commit()
                await db.refresh(db_user)
            return db_user
        else:
            logger.info(f"Creating new user {user_data.telegram_id}")
            new_user = User(telegram_id=user_data.telegram_id, username=user_data.username)
            db.add(new_user)
            await db.commit()
            await db.refresh(new_user)
            return new_user

    async def get_or_create_user_by_id(self, db: AsyncSession, target_user_id: int) -> User:
        stmt = select(User).where(User.telegram_id == target_user_id)
        result = await db.scalars(stmt)
        db_user = result.first()

        if db_user:
            logger.info(f"User {target_user_id} found.")
            return db_user
        else:
            logger.info(f"User {target_user_id} not found. Creating.")
            new_user = User(telegram_id=target_user_id, username=None)
            db.add(new_user)
            await db.commit()
            await db.refresh(new_user)
            return new_user

    async def delete_user_by_id(self, db: AsyncSession, target_user_id: int) -> bool:
        stmt = delete(User).where(User.telegram_id == target_user_id)
        result = await db.execute(stmt)
        await db.commit()

        deleted_count = result.rowcount
        if deleted_count > 0:
            logger.info(f"User {target_user_id} deleted successfully.")
            return True
        else:
            logger.warning(f"User {target_user_id} not found for deletion.")
            return False

    async def get_all_users_except_admins(self, db: AsyncSession, admin_ids: List[int]) -> List[User]:
        stmt = select(User).where(User.telegram_id.notin_(admin_ids))
        result = await db.scalars(stmt)
        users = result.all()
        logger.info(f"Fetched {len(users)} non-admin users.")
        return list(users)


user_service = UserService()