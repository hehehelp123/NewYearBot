from datetime import datetime
from pydantic import BaseModel, ConfigDict, HttpUrl

class WishlistAddRequest(BaseModel):
    source_url: HttpUrl
    telegram_id: int

# --- Booking Schemas ---
class ItemBookingInfo(BaseModel):
    booked_by_user_id: int
    booked_at: datetime

class ItemBookRequest(BaseModel):
    item_id: int
    booker_user_id: int

class WishlistGetRequest(BaseModel):
    owner_user_id: int
    requester_user_id: int

# --- Item Schemas ---
class WishlistItemBase(BaseModel):
    name: str
    item_url: str | None = None

class WishlistItemCreate(WishlistItemBase):
    pass

class WishlistItemForOwner(WishlistItemBase):
    item_id: int
    added_at: datetime
    model_config = ConfigDict(from_attributes=True)

class WishlistItemForViewer(WishlistItemForOwner):
    booking: ItemBookingInfo | None = None

# --- Wishlist Schemas ---
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