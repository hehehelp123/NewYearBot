from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from app.models.ticket_models import Ticket
from app.schemas.ticket_schemas import TicketCreate

class TicketService:
    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session

    async def get_ticket_by_id(self, ticket_id: int) -> Ticket | None:
        query = (
            select(Ticket)
            .where(Ticket.ticket_id == ticket_id)
            .options(selectinload(Ticket.comments))
        )
        result = await self.db_session.execute(query)
        return result.scalar_one_or_none()


    async def create_ticket(self, ticket: TicketCreate) -> Ticket | None:
        db_ticket = Ticket(**ticket.model_dump())
        self.db_session.add(db_ticket)
        await self.db_session.commit()
        await self.db_session.refresh(db_ticket)

        return await self.get_ticket_by_id(db_ticket.ticket_id)