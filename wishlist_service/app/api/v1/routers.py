from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError
from app.core.db import get_db
from app.schemas.wishlist_schemas import WishlistCreate, WishlistForOwner, WishlistForViewer
from app.services.wishlist_service import WishlistService

router = APIRouter()

@router.get("/features")
def get_wishlist_service_features():
    return [
        {
            "name": "🎁 Вишлисты",
            "type": "menu",
            "items": [
                {
                    "name": "Добавить в вишлист",
                    "type": "action",
                    "kafka_topic": "wishlist.wishlist.add",
                    "payload": {
                        "source_url": {"type": "string", "description": "Ссылка на товар ozon/wildberries/aliexpress/yandex market"}
                    },
                },
                {
                    "name": "Просмотреть вишлист пользователя",
                    "type": "action",
                    "unfinished": True,
                    "kafka_topic": "wishlist.view.viewer",
                    "payload": {
                        "target_user": {"type": "string", "description": "Введите тэг пользователя"}
                    },
                },
                {
                    "name": "Просмотреть забронированные товары",
                    "type": "action",
                    "unfinished": True,
                    "kafka_topic": "wishlist.view.booked_items",
                },
                {
                    "name": "Просмотреть вишлисты",
                    "type": "action",
                    "unfinished": True,
                    "kafka_topic": "wishlist.view.all",
                },
            ]
        }
    ]


@router.get("/")
def read_root():
    return {"service": "Wishlist Service", "status": "ok"}

@router.post("/wishlists/", response_model=WishlistForOwner)
async def create_wishlist(wishlist: WishlistCreate, db: AsyncSession = Depends(get_db)):
    wishlist_service = WishlistService(db)
    try:
        new_wishlist = await wishlist_service.create_wishlist(wishlist)
        return new_wishlist
    except IntegrityError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database integrity error."
        )

@router.get("/wishlists/{wishlist_id}")
async def get_wishlist(wishlist_id: int, viewer_user_id: int, db: AsyncSession = Depends(get_db)):
    wishlist_service = WishlistService(db)
    wishlist = await wishlist_service.get_wishlist_by_id(wishlist_id)

    if not wishlist:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Wishlist not found")

    if wishlist.owner_user_id == viewer_user_id:
        return WishlistForOwner.model_validate(wishlist)
    else:
        return WishlistForViewer.model_validate(wishlist)