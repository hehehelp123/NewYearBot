import logging
import tempfile
import os
import re
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.kafka.producer import kafka_producer
from app.models.ticket_models import Ticket
from app.schemas.ticket_schemas import TicketCreate
from app.services.storage_service import storage_service
from app.services.pdf_parser import parse_rzd_ticket

logger = logging.getLogger(__name__)


def escape_markdown(text: str) -> str:
    if not isinstance(text, str):
        return ""
    escape_chars = r"[_*\[\]()~`>#\+\-=|{}.!]"
    return re.sub(f"({escape_chars})", r"\\\1", text)


class TicketService:
    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session

    async def publish_active_tickets_list(self, user_id: int):
        query = (
            select(Ticket)
            .where(
                Ticket.requester_user_id == user_id,
                Ticket.departure_datetime >= datetime.utcnow()
            )
            .order_by(Ticket.departure_datetime.asc())
        )
        result = await self.db_session.execute(query)
        tickets = result.scalars().all()

        ticket_list_for_kafka = []
        for ticket in tickets:
            ticket_list_for_kafka.append({
                "ticket_id": ticket.ticket_id,
                "title": ticket.title,
                "passenger_name": ticket.passenger_name,
                "train_number": ticket.train_number,
                "wagon_number": ticket.wagon_number,
                "seat_number": ticket.seat_number,
                "departure_station": ticket.departure_station,
                "departure_datetime": ticket.departure_datetime.isoformat() if ticket.departure_datetime else None,
                "arrival_station": ticket.arrival_station,
                "arrival_datetime": ticket.arrival_datetime.isoformat() if ticket.arrival_datetime else None,
            })

        message = {"telegram_id": user_id, "tickets": ticket_list_for_kafka}
        await kafka_producer.send("notification.send.tickets", message)

    async def send_ticket_document(self, user_id: int, ticket_id: int):
        ticket = await self.get_ticket_by_id(ticket_id)
        if not ticket or not ticket.storage_key:
            message = {"chat_id": user_id, "text": f"Не удалось найти файл для билета ID {ticket_id}\\."}
            await kafka_producer.send("notification.send", message)
            return

        caption = f"Ваш билет: *{escape_markdown(ticket.title)}*"
        message = {
            "chat_id": user_id,
            "storage_key": ticket.storage_key,
            "caption": caption,
            "filename": f"ticket_{ticket.ticket_id}.pdf"
        }
        await kafka_producer.send("notification.send.document", message)

    async def delete_ticket_by_id(self, user_id: int, ticket_id: int):
        query = select(Ticket).where(Ticket.ticket_id == ticket_id, Ticket.requester_user_id == user_id)
        result = await self.db_session.execute(query)
        ticket = result.scalar_one_or_none()

        if not ticket:
            message = {"chat_id": user_id, "text": f"Билет с ID {ticket_id} не найден\\."}
            await kafka_producer.send("notification.send", message)
            return

        title = escape_markdown(ticket.title)
        if ticket.storage_key:
            try:
                storage_service.delete_file(ticket.storage_key)
            except Exception as e:
                logger.error(f"Could not delete file {ticket.storage_key} from MinIO: {e}", exc_info=True)

        await self.db_session.delete(ticket)
        await self.db_session.commit()

        message = {"chat_id": user_id, "text": f"🗑️ Билет *{title}* был удален\\."}
        await kafka_producer.send("notification.send", message)
        logger.info(f"Ticket {ticket_id} deleted for user {user_id}.")

    async def get_ticket_by_id(self, ticket_id: int) -> Ticket | None:
        query = (select(Ticket).where(Ticket.ticket_id == ticket_id))
        result = await self.db_session.execute(query)
        return result.scalar_one_or_none()

    async def process_ticket_creation_request(self, event_data: dict):
        storage_key = event_data.get("ticket_file")
        title = event_data.get("title")
        telegram_id = event_data.get("telegram_id")

        if not all([storage_key, title, telegram_id]):
            logger.error(f"Missing required fields in ticket creation event: {event_data}")
            return

        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
                tmp_path = tmp.name

            logger.info(f"Downloading file {storage_key} to temporary path {tmp_path}")
            storage_service.download_file_to_path(storage_key, tmp_path)

            with open(tmp_path, "rb") as f:
                parsed_data = parse_rzd_ticket(f)

            ticket_data = TicketCreate(requester_user_id=telegram_id, title=title)
            db_ticket = Ticket(
                **ticket_data.model_dump(),
                storage_key=storage_key,
                **parsed_data
            )
            self.db_session.add(db_ticket)
            await self.db_session.commit()
            await self.db_session.refresh(db_ticket)
            logger.info(f"Ticket {db_ticket.ticket_id} created for user {telegram_id}.")

            ticket_payload = {
                "telegram_id": db_ticket.requester_user_id,
                "ticket_id": db_ticket.ticket_id,
                "title": db_ticket.title,
                "passenger_name": db_ticket.passenger_name,
                "train_number": db_ticket.train_number,
                "wagon_number": db_ticket.wagon_number,
                "seat_number": db_ticket.seat_number,
                "departure_station": db_ticket.departure_station,
                "departure_datetime": db_ticket.departure_datetime.isoformat() if db_ticket.departure_datetime else None,
                "arrival_station": db_ticket.arrival_station,
                "arrival_datetime": db_ticket.arrival_datetime.isoformat() if db_ticket.arrival_datetime else None,
                "storage_key": db_ticket.storage_key,
            }
            await kafka_producer.send("ticket.created", ticket_payload)

        except Exception as e:
            logger.error(f"Failed to process ticket creation for user {telegram_id}: {e}", exc_info=True)
            error_message = {
                "chat_id": telegram_id,
                "text": "Произошла ошибка при обработке вашего билета\\. Пожалуйста, попробуйте еще раз\\."
            }
            await kafka_producer.send("notification.send", error_message)
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)


async def process_ticket_list_request(self, event_data: dict):
    telegram_id = event_data.get("telegram_id")
    if not telegram_id:
        logger.warning(f"Received ticket list request with no telegram_id: {event_data}")
        return

    try:
        query = (
            select(Ticket)
            .where(
                Ticket.requester_user_id == telegram_id,
                Ticket.departure_datetime >= datetime.utcnow()
            )
            .order_by(Ticket.departure_datetime.asc())
        )
        result = await self.db_session.execute(query)
        tickets = result.scalars().all()

        ticket_list_for_kafka = [
            {
                "id": ticket.ticket_id,
                "title": ticket.title,
                "passenger_name": ticket.passenger_name,
                "train_number": ticket.train_number,
                "wagon_number": ticket.wagon_number,
                "seat_number": ticket.seat_number,
                "departure_station": ticket.departure_station,
                "departure_datetime": ticket.departure_datetime.isoformat() if ticket.departure_datetime else None,
                "arrival_station": ticket.arrival_station,
                "arrival_datetime": ticket.arrival_datetime.isoformat() if ticket.arrival_datetime else None,
            } for ticket in tickets
        ]

        response_payload = {"chat_id": telegram_id, "tickets": ticket_list_for_kafka}

        await kafka_producer.send("notification.send.tickets", response_payload)
        logger.info(f"Successfully sent ticket list for user {telegram_id}")

    except Exception as e:
        logger.error(f"Failed to process ticket list request for user {telegram_id}: {e}", exc_info=True)
        error_message = {
            "chat_id": telegram_id,
            "text": "Произошла ошибка при получении списка билетов\\."
        }
        await kafka_producer.send("notification.send", error_message)