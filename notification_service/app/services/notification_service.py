from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.notification_models import ScheduledNotification
from app.schemas.notification_schemas import NotificationCreate


class NotificationService:
    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session

    async def create_reminder(self, reminder: NotificationCreate) -> ScheduledNotification:
        db_reminder = ScheduledNotification(**reminder.model_dump())
        self.db_session.add(db_reminder)
        await self.db_session.commit()
        await self.db_session.refresh(db_reminder)
        return db_reminder

    async def create_reminder_from_event(self, event_data: dict, event_type: str):
        if event_type == "ticket_created":
            if "requester_user_id" in event_data and "event_date" in event_data:
                user_id = event_data["requester_user_id"]
                event_date_str = event_data["event_date"]

                if event_date_str:
                    event_date = datetime.fromisoformat(event_date_str)
                    reminder_date = event_date - timedelta(days=3)

                    message = f"Напоминание: у вас мероприятие '{event_data.get('title', '')}' через 3 дня!"

                    reminder_schema = NotificationCreate(
                        user_id=user_id,
                        message=message,
                        send_at=reminder_date
                    )
                    await self.create_reminder(reminder_schema)

        elif event_type == "music_upload_status":
            if event_data.get("status") == "success":
                user_id = event_data["user_id"]
                message = f"Ваш трек '{event_data.get('title', '')}' успешно загружен!"

                reminder_schema = NotificationCreate(
                    user_id=user_id,
                    message=message,
                    send_at=datetime.utcnow()
                )
                await self.create_reminder(reminder_schema)