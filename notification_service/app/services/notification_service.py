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


def _is_flight(ticket: dict) -> bool:
    if ticket.get("transport"):
        return ticket["transport"] == "flight"
    return not ticket.get("wagon_number") and bool(re.fullmatch(r"[A-Z0-9]{2} ?\d{2,4}", ticket.get("train_number") or ""))


def _trip_line(ticket: dict) -> str:
    """«Поезд 347АА, вагон 11, место 026» / «Рейс SU 1347» — только известные части, уже экранировано."""
    number = ticket.get("train_number")
    if _is_flight(ticket):
        parts = [f"Рейс {number}"] if number else []
        if ticket.get("seat_number"):
            parts.append(f"место {ticket['seat_number']}")
    else:
        parts = [f"Поезд {number}"] if number else []
        if ticket.get("wagon_number"):
            parts.append(f"вагон {ticket['wagon_number']}")
        if ticket.get("seat_number"):
            parts.append(f"место {ticket['seat_number']}")
    return escape_markdown(", ".join(parts))


def _plural_tracks(n: int) -> str:
    if n % 10 == 1 and n % 100 != 11:
        return f"{n} трек"
    if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:
        return f"{n} трека"
    return f"{n} треков"


def _music_message(event_type: str, data: dict) -> str:
    """MarkdownV2-текст по итогам добавления музыки."""
    e = escape_markdown
    if event_type.endswith(".failed"):
        reason = data.get("reason") or "что-то пошло не так"
        return f"❌ Не получилось добавить музыку: {e(reason)}"

    titles = data.get("titles") or ([data["title"]] if data.get("title") else [])
    shown = "\n".join(f"• {e(t)}" for t in titles[:10])
    if len(titles) > 10:
        shown += f"\n…и ещё {len(titles) - 10}"

    if event_type == "selenium.upload.success":
        text = f"✅ Скачал и загрузил в плейлист {_plural_tracks(len(titles))}:\n{shown}"
        failed = data.get("failed_titles") or []
        if failed:
            text += f"\n\n⚠️ Не загрузились: {e(', '.join(failed[:5]))}"
        return text

    playlist = e(data.get("destination_playlist") or "плейлист")
    added, already = data.get("added_count", 0), data.get("already_count", 0)
    if not added:
        return f"👌 Это уже есть в плейлисте «{playlist}»:\n{shown}"
    head = f"✅ Добавил в «{playlist}» {_plural_tracks(added)}"
    if data.get("matched_query"):
        head = f"✅ Нашёл в Яндекс Музыке и добавил в «{playlist}»"
    text = f"{head}:\n{shown}"
    if already:
        text += f"\n\n{_plural_tracks(already)} уже были в плейлисте"
    return text


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

        elif event_type in ("selenium.upload.success", "yandex.upload.success",
                            "selenium.upload.failed", "yandex.upload.failed"):
            await self._send_immediate(chat_id, _music_message(event_type, event_data))

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
        """Подтверждение + напоминания. Даты в билете — местное время, планировщик работает в UTC."""
        user_id = ticket["telegram_id"]
        offset = timedelta(hours=int(ticket.get("utc_offset") or 3))

        def parse_dt(dt_str):
            return datetime.fromisoformat(dt_str) if dt_str else None

        def format_dt(dt):
            return escape_markdown(dt.strftime('%d.%m.%Y в %H:%M')) if dt else "не распознано"

        departure_local = parse_dt(ticket.get("departure_datetime"))
        arrival_local = parse_dt(ticket.get("arrival_datetime"))

        lines = ["✅ *Билет добавлен\\!*", ""]
        trip = _trip_line(ticket)
        if trip:
            lines += [trip, ""]
        lines.append(f"*Отправление:* {escape_markdown(ticket.get('departure_station') or 'не распознано')}")
        lines.append(f"*Когда:* {format_dt(departure_local)}")
        lines.append("")
        lines.append(f"*Прибытие:* {escape_markdown(ticket.get('arrival_station') or 'не распознано')}")
        lines.append(f"*Когда:* {format_dt(arrival_local)}")
        await self._send_immediate(user_id, "\n".join(lines))

        if not departure_local:
            await self._send_immediate(
                user_id,
                "⚠️ Не смог распознать дату отправления в этом PDF, поэтому напоминаний не будет\\. "
                "Сам билет сохранён — его можно скачать из списка билетов\\."
            )
            return

        now_utc = datetime.utcnow()
        departure_utc = departure_local - offset
        if departure_utc <= now_utc:
            await self._send_immediate(user_id, "ℹ️ Эта поездка уже в прошлом, напоминания не ставлю\\.")
            return

        all_recipients = list(dict.fromkeys([user_id] + list(settings.ADMIN_TELEGRAM_IDS)))
        reminder_days = []
        for days in [3, 1]:
            remind_utc = departure_utc - timedelta(days=days)
            if remind_utc > now_utc:
                reminder_days.append(departure_local - timedelta(days=days))
                time_left = f"{days} дня" if days > 1 else "1 день"
                for recipient_id in all_recipients:
                    await self._schedule_text_reminder(recipient_id, ticket, remind_utc, departure_local, time_left)

        # PDF в день поездки: в 08:00 по местному времени, но не позже чем за 2 часа до отправления
        doc_local = departure_local.replace(hour=8, minute=0, second=0, microsecond=0)
        if departure_local - doc_local < timedelta(hours=2):
            doc_local = departure_local - timedelta(hours=2)
        doc_utc = max(doc_local - offset, now_utc)
        for recipient_id in all_recipients:
            await self._schedule_document_reminder(recipient_id, ticket, doc_utc, departure_local)

        info = []
        if reminder_days:
            dates = ", ".join(d.strftime('%d.%m') for d in sorted(reminder_days))
            info.append(f"🔔 Напоминания тебе и админам: *{escape_markdown(dates)}*\\.")
        doc_when = (doc_utc + offset).strftime('%d.%m в %H:%M')
        info.append(f"📄 PDF билета пришлю *{escape_markdown(doc_when)}*\\.")
        await self._send_immediate(user_id, "\n".join(info))

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

    async def _schedule_text_reminder(self, user_id: int, ticket: dict, send_at_utc: datetime,
                                      departure_local: datetime, time_left: str):
        message = (
            f"❗️ Напоминание о поездке: *{escape_markdown(ticket['title'])}*\n"
            f"Пассажир: {escape_markdown(ticket.get('passenger_name') or 'н/д')}\n"
            f"{_trip_line(ticket) or ''}\n"
            f"Отправление: {escape_markdown(ticket.get('departure_station') or 'н/д')} "
            f"{escape_markdown(departure_local.strftime('%d.%m в %H:%M'))}\n"
            f"До отправления — *{escape_markdown(time_left)}*\\."
        )
        await self._create_scheduled_text({"telegram_id": user_id, "message": message, "send_at": send_at_utc.isoformat()})

    async def _schedule_document_reminder(self, user_id: int, ticket: dict, send_at_utc: datetime,
                                          departure_local: datetime):
        icon = "✈️" if _is_flight(ticket) else "🚆"
        caption = (
            f"{icon} *Билет на сегодня: {escape_markdown(ticket['title'])}*\n"
            f"Пассажир: {escape_markdown(ticket.get('passenger_name') or 'н/д')}\n"
            f"{_trip_line(ticket) or ''}\n"
            f"Отправление: {escape_markdown(ticket.get('departure_station') or 'н/д')}\n"
            f"Время: {escape_markdown(departure_local.strftime('%H:%M'))}"
        )
        payload = {
            "telegram_id": user_id,
            "storage_key": ticket["storage_key"],
            "caption": caption,
            "send_at": send_at_utc.isoformat()
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
