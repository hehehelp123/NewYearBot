import logging
import tempfile
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete
from sqlalchemy.orm import joinedload
from app.models.ticket_models import Ticket
from app.utils.ticket_parser import parse_rzd_ticket
from app.schemas.ticket_schemas import TicketCreate
from typing import List, Optional

logger = logging.getLogger(__name__)


class TicketService:

    async def create_ticket(
            self,
            db: AsyncSession,
            title: str,
            storage_key: str,
            user_id: int,
            temp_file_path: str
    ) -> Ticket:
        logger.info(f"Parsing ticket data from {temp_file_path}")

        parsed_data = {}
        try:
            with open(temp_file_path, "rb") as f_in:
                parsed_data = parse_rzd_ticket(f_in)
        except Exception as e:
            logger.error(f"Failed to parse PDF {temp_file_path}: {e}", exc_info=True)

        ticket_data = TicketCreate(
            title=title,
            storage_key=storage_key,
            user_id=user_id,
            departure_station=parsed_data.get("departure_station"),
            arrival_station=parsed_data.get("arrival_station"),
            departure_datetime=parsed_data.get("departure_datetime"),
            arrival_datetime=parsed_data.get("arrival_datetime"),
            passenger_name=parsed_data.get("passenger_name")
        )

        new_ticket = Ticket(**ticket_data.model_dump())
        db.add(new_ticket)
        await db.flush()
        await db.refresh(new_ticket)
        logger.info(f"Ticket {new_ticket.id} created in DB for user {user_id}")
        return new_ticket

    async def get_tickets_by_user(self, db: AsyncSession, user_id: int) -> List[Ticket]:
        logger.info(f"Fetching tickets for user {user_id}")
        stmt = (
            select(Ticket)
            .where(Ticket.user_id == user_id)
            .order_by(Ticket.departure_datetime.desc())
        )
        result = await db.scalars(stmt)
        tickets = result.all()
        logger.info(f"Found {len(tickets)} tickets for user {user_id}")
        return list(tickets)

    async def get_ticket_by_id(self, db: AsyncSession, ticket_id: int, user_id: int) -> Optional[Ticket]:
        logger.info(f"Fetching ticket {ticket_id} for user {user_id}")
        stmt = (
            select(Ticket)
            .where(Ticket.id == ticket_id, Ticket.user_id == user_id)
        )
        result = await db.scalars(stmt)
        ticket = result.first()
        if ticket:
            logger.info(f"Found ticket {ticket_id}")
        else:
            logger.warning(f"Ticket {ticket_id} not found for user {user_id}")
        return ticket

    async def delete_ticket(self, db: AsyncSession, ticket: Ticket):
        logger.info(f"Deleting ticket {ticket.id} from DB")
        await db.delete(ticket)
        await db.flush()
        logger.info(f"Ticket {ticket.id} deleted from DB")


ticket_service = TicketService()