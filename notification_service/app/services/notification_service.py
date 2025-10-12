import logging
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.kafka.producer import kafka_producer
from app.models.notification_models import ScheduledNotification
from app.schemas.notification_schemas import NotificationCreate

logger = logging.getLogger(__name__)


class NotificationService:
    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session

    async def create_and_send_notification(self, event_data: dict, event_type: str):
        # Используем telegram_id как основной идентификатор для отправки
        chat_id = event_data.get("telegram_id")
        if not chat_id:
            logger.warning(f"Не найден telegram_id в событии {event_type}")
            return

        message = ""
        send_at = datetime.utcnow()
        is_scheduled = False

        if event_type == "user.user.created":
            username = event_data.get("username", "Anonymous")
            message = f"Добро пожаловать в систему, {username}!"

        elif event_type == "selenium.upload.success":
            title = event_data.get("title", "Неизвестный трек")
            message = f"✅ Трек '{title}' успешно скачан и загружен в ваш плейлист!"

        elif event_type == "ticket.ticket.created":
            event_date_str = event_data.get("event_date")
            if event_date_str:
                event_date = datetime.fromisoformat(event_date_str)
                reminder_date = event_date - timedelta(days=3)
                if reminder_date > datetime.utcnow():
                    send_at = reminder_date
                    is_scheduled = True
                message = f"Напоминание: у вас мероприятие '{event_data.get('title', '')}' через 3 дня!"
            else:
                message = f"Ваш тикет '{event_data.get('title', '')}' создан."

        if not message:
            return

        if is_scheduled:
            logger.info(f"Планируем уведомление для chat_id {chat_id} на {send_at}")
            reminder_schema = NotificationCreate(user_id=chat_id, message=message, send_at=send_at)
            db_reminder = ScheduledNotification(**reminder_schema.model_dump())
            self.db_session.add(db_reminder)
            await self.db_session.commit()
        else:
            logger.info(f"Генерируем немедленное уведомление для chat_id {chat_id}")
            notification_payload = {"chat_id": chat_id, "text": message}
            await kafka_producer.send("notification.send", notification_payload)

    async def process_scheduled_notifications(self):
        logger.info("Проверка запланированных уведомлений...")
        query = select(ScheduledNotification).where(
            ScheduledNotification.status == 'pending',
            ScheduledNotification.send_at <= datetime.utcnow()
        )
        result = await self.db_session.execute(query)
        notifications_to_send = result.scalars().all()

        if not notifications_to_send:
            return

        logger.info(f"Найдено {len(notifications_to_send)} уведомлений для отправки.")
        for notif in notifications_to_send:
            payload = {"chat_id": notif.user_id, "text": notif.message}
            await kafka_producer.send("notification.send", payload)
            notif.status = 'sent'

        await self.db_session.commit()