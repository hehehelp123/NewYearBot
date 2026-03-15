from aiogram.fsm.state import State, StatesGroup

class ActionForm(StatesGroup):
    waiting_for_field = State()

class WishlistBrowser(StatesGroup):
    browsing = State()

class WishlistAddManual(StatesGroup):
    waiting_for_name = State()
    waiting_for_cost = State()
    waiting_for_url = State()
    waiting_for_delivery = State()
    confirming = State()

class AllWishlistsBrowser(StatesGroup):
    choosing_owner = State()

class MediaUpload(StatesGroup):
    choosing_album = State()
    waiting_for_new_album_name = State()
    uploading = State()

class AlbumBrowser(StatesGroup):
    choosing_album = State()
    browsing = State()

class UserRemoval(StatesGroup):
    choosing_user = State()
    confirming_delete = State()

class BookedItemsBrowser(StatesGroup):
    browsing = State()