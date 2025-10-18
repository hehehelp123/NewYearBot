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
    storage_key = Column(String, nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)

    passenger_name = Column(String, nullable=True)
    train_number = Column(String, nullable=True)
    wagon_number = Column(String, nullable=True)
    seat_number = Column(String, nullable=True)
    departure_station = Column(String, nullable=True)
    departure_datetime = Column(DateTime, nullable=True)
    arrival_station = Column(String, nullable=True)
    arrival_datetime = Column(DateTime, nullable=True)

    comments = relationship("TicketComment", back_populates="ticket")

class TicketComment(Base):
    __tablename__ = 'ticket_comments'
    comment_id = Column(Integer, primary_key=True)
    ticket_id = Column(Integer, ForeignKey('tickets.ticket_id'), nullable=False)
    author_user_id = Column(Integer, nullable=False)
    text = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    ticket = relationship("Ticket", back_populates="comments")