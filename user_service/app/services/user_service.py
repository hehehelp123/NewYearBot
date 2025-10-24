import logging
from sqlalchemy.orm import Session
from sqlalchemy import select, exists
from app.models.user_models import User
from app.schemas.user_schemas import UserCreateRequest

logger = logging.getLogger(__name__)

class UserService:
    async def create_or_update_user(self, db: Session, user_data: UserCreateRequest) -> User:
        stmt = select(User).where(User.telegram_id == user_data.telegram_id)
        db_user = db.scalars(stmt).first()

        if db_user:
            logger.info(f"User with telegram_id {user_data.telegram_id} already exists. Updating username if changed.")
            if user_data.username and db_user.username != user_data.username:
                db_user.username = user_data.username
                db.commit()
                db.refresh(db_user)
            return db_user
        else:
            logger.info(f"Creating new user with telegram_id {user_data.telegram_id}")
            new_user = User(telegram_id=user_data.telegram_id, username=user_data.username, is_allowed=False)
            db.add(new_user)
            db.commit()
            db.refresh(new_user)
            return new_user

    async def allow_user_by_id(self, db: Session, target_user_id: int) -> User:
        stmt = select(User).where(User.telegram_id == target_user_id)
        db_user = db.scalars(stmt).first()

        if db_user:
            logger.info(f"User with telegram_id {target_user_id} already exists.")
            return db_user
        else:
            logger.info(f"Creating new allowed user with telegram_id {target_user_id}")
            new_user = User(telegram_id=target_user_id, username=None)
            db.add(new_user)
            db.commit()
            db.refresh(new_user)
            return new_user

    async def user_exists(self, db: Session, user_id: int) -> bool:
        stmt = select(exists().where(User.telegram_id == user_id))
        return db.scalar(stmt)


user_service = UserService()