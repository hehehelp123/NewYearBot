from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from app.models.wishlist_models import Wishlist, WishlistItem
from app.schemas.wishlist_schemas import WishlistCreate


class WishlistService:
    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session

    async def get_wishlist_by_id(self, wishlist_id: int) -> Wishlist | None:
        query = (
            select(Wishlist)
            .where(Wishlist.wishlist_id == wishlist_id)
            .options(selectinload(Wishlist.items).selectinload(WishlistItem.booking))
        )
        result = await self.db_session.execute(query)
        return result.scalar_one_or_none()

    async def create_wishlist(self, wishlist: WishlistCreate) -> Wishlist | None:
        db_wishlist = Wishlist(**wishlist.model_dump())
        self.db_session.add(db_wishlist)
        await self.db_session.commit()
        await self.db_session.refresh(db_wishlist)

        return await self.get_wishlist_by_id(db_wishlist.wishlist_id)