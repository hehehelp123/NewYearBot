from sqlalchemy import Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql import func

Base = declarative_base()

class Ticket(Base):
    __tablename__ = 'tickets'
    ticket_id = Column(Integer, primary_key=True)
    requester_user_id = Column(Integer, nullable=False)
    title = Column(String, nullable=False)
    status = Column(String, nullable=False, default='open')
    event_date = Column(DateTime, nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    comments = relationship("TicketComment", back_populates="ticket")


class TicketComment(Base):
    __tablename__ = 'ticket_comments'
    comment_id = Column(Integer, primary_key=True)
    ticket_id = Column(Integer, ForeignKey('tickets.ticket_id'), nullable=False)
    author_user_id = Column(Integer, nullable=False)
    text = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    ticket = relationship("Ticket", back_populates="comments")