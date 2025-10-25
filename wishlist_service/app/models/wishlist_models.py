from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Boolean
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql import func

Base = declarative_base()

class Wishlist(Base):
    __tablename__ = 'wishlists'
    wishlist_id = Column(Integer, primary_key=True)
    owner_user_id = Column(Integer, nullable=False)
    name = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    items = relationship("WishlistItem", back_populates="wishlist")

class WishlistItem(Base):
    __tablename__ = 'wishlist_items'
    item_id = Column(Integer, primary_key=True)
    wishlist_id = Column(Integer, ForeignKey('wishlists.wishlist_id'), nullable=False)
    name = Column(String, nullable=False)
    item_url = Column(String, nullable=True)
    added_at = Column(DateTime, server_default=func.now(), nullable=False)
    wishlist = relationship("Wishlist", back_populates="items")
    bookings = relationship("ItemBooking", back_populates="item")
    cost = Column(String, nullable=True)
    delivery_date = Column(String, nullable=True)
    is_infinitely_bookable = Column(Boolean, default=False, nullable=False)

class ItemBooking(Base):
    __tablename__ = 'item_bookings'
    booking_id = Column(Integer, primary_key=True)
    item_id = Column(Integer, ForeignKey('wishlist_items.item_id'), nullable=False)
    booked_by_user_id = Column(Integer, nullable=False)
    booked_at = Column(DateTime, server_default=func.now(), nullable=False)
    item = relationship("WishlistItem", back_populates="bookings")