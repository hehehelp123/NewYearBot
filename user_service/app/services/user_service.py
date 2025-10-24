import logging
from sqlalchemy.orm import Session
from sqlalchemy import select, exists, delete
from app.models.user_models import User
from app.schemas.user_schemas import UserCreateRequest
from typing import List

logger = logging.getLogger(__name__)

class UserService:
    async def create_or_update_user(self, db: Session, user_data: UserCreateRequest) -> User:
        stmt = select(User).where(User.telegram_id == user_data.telegram_id)
        db_user = db.scalars(stmt).first()

        if db_user:
            logger.info(f"User {user_data.telegram_id} exists. Updating username.")
            if user_data.username and db_user.username != user_data.username:
                db_user.username = user_data.username
                db.commit()
                db.refresh(db_user)
            return db_user
        else:
            logger.info(f"Creating new user {user_data.telegram_id}")
            new_user = User(telegram_id=user_data.telegram_id, username=user_data.username)
            db.add(new_user)
            db.commit()
            db.refresh(new_user)
            return new_user

    async def get_or_create_user_by_id(self, db: Session, target_user_id: int) -> User:
        stmt = select(User).where(User.telegram_id == target_user_id)
        db_user = db.scalars(stmt).first()

        if db_user:
            logger.info(f"User {target_user_id} found.")
            return db_user
        else:
            logger.info(f"User {target_user_id} not found. Creating.")
            new_user = User(telegram_id=target_user_id, username=None) # Username unknown initially
            db.add(new_user)
            db.commit()
            db.refresh(new_user)
            return new_user

    async def delete_user_by_id(self, db: Session, target_user_id: int) -> bool:
        stmt = delete(User).where(User.telegram_id == target_user_id)
        result = db.execute(stmt)
        db.commit()
        deleted_count = result.rowcount
        if deleted_count > 0:
            logger.info(f"User {target_user_id} deleted successfully.")
            return True
        else:
            logger.warning(f"User {target_user_id} not found for deletion.")
            return False

    async def get_all_users_except_admins(self, db: Session, admin_ids: List[int]) -> List[User]:
        stmt = select(User).where(User.telegram_id.notin_(admin_ids))
        users = db.scalars(stmt).all()
        logger.info(f"Fetched {len(users)} non-admin users.")
        return list(users)

user_service = UserService()