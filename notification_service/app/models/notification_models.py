from sqlalchemy import Column, DateTime, Integer, String, Text
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()

class ScheduledNotification(Base):
    __tablename__ = 'scheduled_notifications'
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False, index=True)
    message = Column(Text)
    send_at = Column(DateTime, nullable=False, index=True)
    status = Column(String, default='pending', index=True)
    created_at = Column(DateTime, server_default=func.now())

    storage_key = Column(String, nullable=True)
    document_caption = Column(Text, nullable=True)