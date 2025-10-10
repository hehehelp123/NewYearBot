from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()

class ScheduledNotification(Base):
    __tablename__ = 'scheduled_notifications'

    notification_id = Column(Integer, primary_key=True)
    user_id = Column(Integer, nullable=False, index=True)
    message = Column(String, nullable=False)
    send_at = Column(DateTime, nullable=False, index=True)
    status = Column(String, nullable=False, default='pending', index=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)