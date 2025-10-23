import logging
import re
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.core.config import settings
from app.kafka.producer import kafka_producer
from app.models.notification_models import ScheduledNotification
from app.schemas.notification_schemas import NotificationCreate

logger = logging.getLogger(__name__)


def escape_markdown(text: str) -> str:
    if not isinstance(text, str):
        return ""
    escape_chars = r"[_*\[\]()~`>#\+\-=|{}.!]"
    return re.sub(f"({escape_chars})", r"\\\1", text)


class NotificationService:
    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session

    async def create_and_send_notification(self, event_data: dict, event_type: str):
        chat_id = event_data.get("telegram_id")
        if not chat_id and event_type not in ["ticket.list.retrieved"]:
             chat_id = event_data.get("chat_id")
             if not chat_id:
                logger.warning(f"No chat_id or telegram_id found in event from topic {event_type}")
                return

        if event_type == "user.user.created":
            username = escape_markdown(event_data.get("username", "Anonymous"))
            message = f"Добро пожаловать в систему, {username}\\!"
            await self._send_immediate(chat_id, message)

        elif event_type == "selenium.upload.success":
            title = escape_markdown(event_data.get("title", "Неизвестный трек"))
            message = f"✅ Трек *{title}* успешно скачан и загружен в ваш плейлист\\!"
            await self._send_immediate(chat_id, message)

        elif event_type == "ticket.created":
            await self._handle_ticket_created(event_data)

        elif event_type == "ticket.list.retrieved":
            await self._handle_ticket_list(event_data)

        elif event_type == "notification.schedule":
            await self._create_scheduled_text(event_data)

        elif event_type == "notification.schedule.document":
            await self._create_scheduled_document(event_data)

        elif event_type == "wishlist.wishlist.created":
            name = escape_markdown(event_data.get("name", "My Wishlist"))
            message = f"🎉 Ваш вишлист '*__{name}__*' создан\\! Теперь вы можете добавлять в него товары\\."
            await self._send_immediate(chat_id, message)

        elif event_type == "wishlist.item.added":
            name = escape_markdown(event_data.get("name", "Новый товар"))
            message = f"✅ Товар '*{name}*' добавлен в ваш вишлист\\."
            await self._send_immediate(chat_id, message)

        elif event_type == "wishlist.item.add_failed":
            url = escape_markdown(event_data.get("source_url", "URL"))
            reason = escape_markdown(event_data.get("reason", "Неизвестная ошибка"))
            message = f"❗️ Не удалось добавить товар с {url}\\.\n*Причина:* {reason}"
            await self._send_immediate(chat_id, message)

        elif event_type == "wishlist.item.booked":
            await self._handle_item_booked(event_data)

        elif event_type == "wishlist.item.book_failed":
            reason = escape_markdown(event_data.get("reason", "Неизвестная ошибка"))
            message = f"❗️ Не удалось забронировать товар\\.\n*Причина:* {reason}"
            await self._send_immediate(chat_id, message)

        elif event_type == "wishlist.item.unbooked":
            await self._handle_item_unbooked(event_data)

        elif event_type == "wishlist.item.unbook_failed":
            reason = escape_markdown(event_data.get("reason", "Неизвестная ошибка"))
            message = f"❗️ Не удалось снять бронь\\.\n*Причина:* {reason}"
            await self._send_immediate(chat_id, message)
        
        elif event_type == "wishlist.item.deleted":
            name = escape_markdown(event_data.get("item_name", "Товар"))
            message = f"✅ Товар '*{name}*' был успешно удален из вашего вишлиста\\."
            await self._send_immediate(chat_id, message)

        elif event_type == "wishlist.item.delete_failed":
            reason = escape_markdown(event_data.get("reason", "Неизвестная ошибка"))
            message = f"❗️ Не удалось удалить товар\\.\n*Причина:* {reason}"
            await self._send_immediate(chat_id, message)

        elif event_type in ("wishlist.view.viewer_failed"):
            reason = escape_markdown(event_data.get("reason", "Неизвестная ошибка"))
            message = f"❗️ Не удалось загрузить вишлист\\.\n*Причина:* {reason}"
            await self._send_immediate(chat_id, message)

    async def _handle_ticket_created(self, ticket: dict):
        user_id = ticket["telegram_id"]
        confirmation_message = "✅ *Билет успешно добавлен\\!*\n\n"
        reminders_info = ""

        def parse_dt(dt_str):
            return datetime.fromisoformat(dt_str) if dt_str else None

        def format_dt(dt):
            return escape_markdown(dt.strftime('%d.%m.%Y в %H:%M')) if dt else "Не найдено"

        departure_datetime = parse_dt(ticket.get("departure_datetime"))
        arrival_datetime = parse_dt(ticket.get("arrival_datetime"))

        confirmation_message += f"*Отправление:* {escape_markdown(ticket.get('departure_station', ''))}\n"
        confirmation_message += f"*Дата:* {format_dt(departure_datetime)}\n\n"
        confirmation_message += f"*Прибытие:* {escape_markdown(ticket.get('arrival_station', ''))}\n"
        confirmation_message += f"*Дата:* {format_dt(arrival_datetime)}\n"

        await self._send_immediate(user_id, confirmation_message)

        if not departure_datetime:
            return

        all_recipients = [user_id] + settings.ADMIN_TELEGRAM_IDS
        reminder_dates = []

        for days in [3, 1]:
            reminder_dt = departure_datetime - timedelta(days=days)
            if reminder_dt > datetime.utcnow():
                reminder_dates.append(reminder_dt)
                time_left = f"{days} дня" if days > 1 else "1 день"
                for recipient_id in all_recipients:
                    await self._schedule_text_reminder(recipient_id, ticket, reminder_dt, time_left)

        if reminder_dates:
            formatted_dates = ", ".join([d.strftime('%d.%m.%Y') for d in sorted(reminder_dates)])
            reminders_info = f"\n🔔 Напоминания для вас и администраторов запланированы на: *{escape_markdown(formatted_dates)}*\\."

        for recipient_id in all_recipients:
            await self._schedule_document_reminder(recipient_id, ticket, departure_datetime)

        dep_date_str = departure_datetime.strftime('%d.%m.%Y')
        reminders_info += f"\n📄 Сам билет будет отправлен вам и администраторам в день отправления: *{escape_markdown(dep_date_str)}*\\."

        await self._send_immediate(user_id, reminders_info)

    async def _handle_ticket_list(self, data: dict):
        user_id = data.get("telegram_id")
        tickets = data.get("tickets", [])
        logger.info(f"Passing ticket list for user {user_id} to bot consumer.")
        payload = {"chat_id": user_id, "tickets": tickets}
        await kafka_producer.send("notification.send.tickets", payload)

    async def _handle_item_booked(self, event_data: dict):
        booker_id = event_data.get("telegram_id")
        owner_id = event_data.get("owner_user_id")
        item_name = escape_markdown(event_data.get("item_name", "Товар"))

        if booker_id:
            booker_message = f"🎉 Вы успешно забронировали '*__{item_name}__*'\\!"
            await self._send_immediate(booker_id, booker_message)

    async def _handle_item_unbooked(self, event_data: dict):
        unbooker_id = event_data.get("telegram_id")
        owner_id = event_data.get("owner_user_id")
        item_name = escape_markdown(event_data.get("item_name", "Товар"))

        if unbooker_id:
            unbooker_message = f"✅ Вы сняли бронь с товара '*__{item_name}__*'\\."
            await self._send_immediate(unbooker_id, unbooker_message)

    async def _schedule_text_reminder(self, user_id: int, ticket: dict, send_at: datetime, time_left: str):
        message = (
            f"❗️ Напоминание о поездке: *{escape_markdown(ticket['title'])}*\n"
            f"Пассажир: {escape_markdown(ticket.get('passenger_name', 'н/д'))}\n"
            f"Поезд {escape_markdown(ticket.get('train_number', 'н/д'))}, вагон {escape_markdown(ticket.get('wagon_number', 'н/д'))}, место {escape_markdown(ticket.get('seat_number', 'н/д'))}\n"
            f"Отправление: {escape_markdown(ticket.get('departure_station', 'н/д'))} в {escape_markdown(send_at.strftime('%H:%M'))}\n"
            f"До отправления — *{escape_markdown(time_left)}*\\."
        )
        await self._create_scheduled_text({"telegram_id": user_id, "message": message, "send_at": send_at.isoformat()})

    async def _schedule_document_reminder(self, user_id: int, ticket: dict, send_at: datetime):
        dep_time_str = send_at.strftime('%H:%M') if send_at else 'н/д'
        caption = (
            f"🚆 *Ваш билет на сегодня: {escape_markdown(ticket['title'])}*\n"
            f"Пассажир: {escape_markdown(ticket.get('passenger_name', 'н/д'))}\n"
            f"Поезд {escape_markdown(ticket.get('train_number', 'н/д'))}, вагон {escape_markdown(ticket.get('wagon_number', 'н/д'))}, место {escape_markdown(ticket.get('seat_number', 'н/д'))}\n"
            f"Отправление: {escape_markdown(ticket.get('departure_station', 'н/д'))}\n"
            f"Время: {escape_markdown(dep_time_str)}"
        )
        payload = {
            "telegram_id": user_id,
            "storage_key": ticket["storage_key"],
            "caption": caption,
            "send_at": send_at.isoformat()
        }
        await self._create_scheduled_document(payload)

    async def _send_immediate(self, chat_id: int, text: str):
        await kafka_producer.send("notification.send", {"chat_id": chat_id, "text": text})

    async def _create_scheduled_text(self, event_data: dict):
        send_at = datetime.fromisoformat(event_data["send_at"])
        schema = NotificationCreate(user_id=event_data["telegram_id"], message=event_data["message"], send_at=send_at)
        db_notif = ScheduledNotification(**schema.model_dump())
        self.db_session.add(db_notif)
        await self.db_session.commit()

    async def _create_scheduled_document(self, event_data: dict):
        send_at = datetime.fromisoformat(event_data["send_at"])
        schema = NotificationCreate(user_id=event_data["telegram_id"], send_at=send_at)
        db_notif = ScheduledNotification(
            **schema.model_dump(exclude={"message"}),
            storage_key=event_data["storage_key"],
            document_caption=event_data["caption"]
        )
        self.db_session.add(db_notif)
        await self.db_session.commit()

    async def process_scheduled_notifications(self):
        query = select(ScheduledNotification).where(
            ScheduledNotification.status == 'pending',
            ScheduledNotification.send_at <= datetime.utcnow()
        )
        result = await self.db_session.execute(query)
        notifications = result.scalars().all()

        if not notifications: return
        logger.info(f"Found {len(notifications)} notifications to send.")
        for notif in notifications:
            if notif.storage_key:
                payload = {"chat_id": notif.user_id, "storage_key": notif.storage_key,
                           "caption": notif.document_caption}
                await kafka_producer.send("notification.send.document", payload)
            else:
                payload = {"chat_id": notif.user_id, "text": notif.message}
                await kafka_producer.send("notification.send", payload)
            notif.status = 'sent'
        await self.db_session.commit()
