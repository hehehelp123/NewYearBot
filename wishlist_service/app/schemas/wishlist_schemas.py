from datetime import datetime
from pydantic import BaseModel, ConfigDict, HttpUrl

class WishlistAddRequest(BaseModel):
    source_text: str
    telegram_id: int
    is_infinitely_bookable: bool = False

class ItemManualAddRequest(BaseModel):
    telegram_id: int
    name: str
    item_url: str | None = None
    cost: str | None = None
    delivery_date: str | None = None
    is_infinitely_bookable: bool = False

class ItemBookingInfo(BaseModel):
    booked_by_user_id: int
    booked_at: datetime
    model_config = ConfigDict(from_attributes=True)

class ItemBookRequest(BaseModel):
    item_id: int
    booker_user_id: int

class ItemDeleteRequest(BaseModel):
    item_id: int
    deleter_user_id: int

class WishlistGetRequest(BaseModel):
    owner_user_name: str
    requester_user_id: int

class WishlistItemBase(BaseModel):
    name: str
    item_url: str | None = None
    cost: str | None = None
    delivery_date: str | None = None

class WishlistItemCreate(WishlistItemBase):
    is_infinitely_bookable: bool = False

class WishlistItemForOwner(WishlistItemBase):
    item_id: int
    added_at: datetime
    is_infinitely_bookable: bool
    model_config = ConfigDict(from_attributes=True)

class WishlistItemForViewer(WishlistItemForOwner):
    bookings: list[ItemBookingInfo] = []

class WishlistBase(BaseModel):
    owner_user_id: int
    name: str

class WishlistCreate(WishlistBase):
    pass

class WishlistForOwner(WishlistBase):
    wishlist_id: int
    items: list[WishlistItemForOwner] = []
    model_config = ConfigDict(from_attributes=True)

class WishlistForViewer(WishlistBase):
    wishlist_id: int
    items: list[WishlistItemForViewer] = []
    model_config = ConfigDict(from_attributes=True)