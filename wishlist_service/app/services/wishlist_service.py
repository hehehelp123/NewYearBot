import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from app.models.wishlist_models import Wishlist, WishlistItem, ItemBooking
from app.schemas.wishlist_schemas import (
    WishlistCreate,
    WishlistForOwner,
    WishlistForViewer,
    WishlistItemCreate,
    WishlistItemForOwner,
    ItemManualAddRequest
)
from app.kafka.producer import kafka_producer
from app.services.scraper_service import scraper_service

logger = logging.getLogger(__name__)


class WishlistService:
    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session

    async def get_wishlist_by_id(self, wishlist_id: int) -> Wishlist | None:
        logger.debug(f"Fetching wishlist by id: {wishlist_id}")
        query = (
            select(Wishlist)
            .where(Wishlist.wishlist_id == wishlist_id)
            .options(selectinload(Wishlist.items).selectinload(WishlistItem.bookings))
        )
        result = await self.db_session.execute(query)
        return result.scalar_one_or_none()

    async def get_wishlist_by_owner_id(self, owner_user_id: int) -> Wishlist | None:
        logger.debug(f"Fetching wishlist by owner_user_id: {owner_user_id}")
        query = (
            select(Wishlist)
            .where(Wishlist.owner_user_id == owner_user_id)
            .options(selectinload(Wishlist.items).selectinload(WishlistItem.bookings))
        )
        result = await self.db_session.execute(query)
        return result.scalar_one_or_none()

    async def get_item_by_id(self, item_id: int) -> WishlistItem | None:
        logger.debug(f"Fetching item by id: {item_id}")
        query = (
            select(WishlistItem)
            .where(WishlistItem.item_id == item_id)
            .options(selectinload(WishlistItem.bookings), selectinload(WishlistItem.wishlist))
        )
        result = await self.db_session.execute(query)
        return result.scalar_one_or_none()

    async def create_wishlist(self, wishlist: WishlistCreate) -> Wishlist | None:
        logger.info(f"Attempting to create wishlist for user_id: {wishlist.owner_user_id}")

        existing_wishlist = await self.get_wishlist_by_owner_id(wishlist.owner_user_id)
        if existing_wishlist:
            logger.warning(f"Wishlist already exists for user_id: {wishlist.owner_user_id}")
            wishlist_schema = WishlistForOwner.model_validate(existing_wishlist)
            event_payload = wishlist_schema.model_dump(mode="json")
            event_payload["telegram_id"] = existing_wishlist.owner_user_id
            await kafka_producer.send("wishlist.wishlist.created", event_payload)
            return existing_wishlist

        db_wishlist = Wishlist(**wishlist.model_dump())
        self.db_session.add(db_wishlist)
        await self.db_session.commit()
        await self.db_session.flush()
        logger.info(f"Successfully committed new wishlist to DB for user_id: {wishlist.owner_user_id}")

        created_wishlist = await self.get_wishlist_by_id(db_wishlist.wishlist_id)

        if not created_wishlist:
            logger.error(f"Failed to fetch newly created wishlist with id: {db_wishlist.wishlist_id}")
            return None

        logger.debug(f"Successfully fetched created wishlist with id: {created_wishlist.wishlist_id}")

        wishlist_schema = WishlistForOwner.model_validate(created_wishlist)
        event_payload = wishlist_schema.model_dump(mode="json")
        event_payload["telegram_id"] = created_wishlist.owner_user_id
        await kafka_producer.send("wishlist.wishlist.created", event_payload)
        logger.info(f"Sent 'wishlist.wishlist.created' event for wishlist_id: {created_wishlist.wishlist_id}")

        return created_wishlist

    async def add_to_wishlist(self, source_url: str, user_id: int, is_infinitely_bookable: bool = False):
        logger.info(f"Starting background task to add item from {source_url} for user {user_id}")
        downloaded_data = await scraper_service.parse_url(source_url, user_id)

        logger.info(downloaded_data)

        if not downloaded_data or not downloaded_data.get('name'):
            logger.error(f"Background task: Failed to parse item data from {source_url} for user {user_id}")
            await kafka_producer.send("wishlist.item.parse_failed",
                                      {"telegram_id": user_id, "source_url": source_url,
                                       "reason": "Failed to parse item data. The website structure might have changed."})
            return

        logger.info(f"Data parsed successfully: {downloaded_data.get('name')}")

        wishlist = await self.get_wishlist_by_owner_id(user_id)
        if not wishlist:
            logger.error(f"Background task: No wishlist found for user {user_id} while trying to add item.")
            await kafka_producer.send("wishlist.item.add_failed",
                                      {"telegram_id": user_id, "source_url": source_url,
                                       "reason": f"No wishlist found for user {user_id}."})
            return

        try:
            item_data = WishlistItemCreate(
                name=downloaded_data['name'],
                item_url=downloaded_data.get('item_url', source_url),
                cost=downloaded_data['cost'],
                delivery_date=downloaded_data['delivery_date'],
                is_infinitely_bookable=is_infinitely_bookable
            )

            db_item = WishlistItem(**item_data.model_dump(exclude={"is_infinitely_bookable"}),
                                   wishlist_id=wishlist.wishlist_id,
                                   is_infinitely_bookable=is_infinitely_bookable)

            self.db_session.add(db_item)
            await self.db_session.commit()
            await self.db_session.refresh(db_item)

            logger.info(f"Successfully added new item '{db_item.name}' to wishlist {wishlist.wishlist_id}")

            item_schema = WishlistItemForOwner.model_validate(db_item)
            event_payload = item_schema.model_dump(mode="json")
            event_payload["telegram_id"] = user_id
            await kafka_producer.send("wishlist.item.added", event_payload)

        except Exception as e:
            logger.error(f"Background task: Failed to save item to DB for user {user_id}: {e}", exc_info=True)
            await kafka_producer.send("wishlist.item.add_failed",
                                      {"telegram_id": user_id, "source_url": source_url,
                                       "reason": f"Database error: {e}"})

    async def add_manual_item(self, item_request: ItemManualAddRequest):
        user_id = item_request.telegram_id
        logger.info(f"Starting manual item add for user {user_id} with name {item_request.name}")

        wishlist = await self.get_wishlist_by_owner_id(user_id)
        if not wishlist:
            logger.error(f"Manual add task: No wishlist found for user {user_id}")
            await kafka_producer.send("wishlist.item.add_failed",
                                      {"telegram_id": user_id, "source_url": item_request.item_url or item_request.name,
                                       "reason": f"No wishlist found for user {user_id}."})
            return

        try:
            item_data = item_request.model_dump(exclude={"telegram_id"})
            db_item = WishlistItem(**item_data, wishlist_id=wishlist.wishlist_id)

            self.db_session.add(db_item)
            await self.db_session.commit()
            await self.db_session.refresh(db_item)

            logger.info(f"Successfully added new manual item '{db_item.name}' to wishlist {wishlist.wishlist_id}")

            item_schema = WishlistItemForOwner.model_validate(db_item)
            event_payload = item_schema.model_dump(mode="json")
            event_payload["telegram_id"] = user_id
            await kafka_producer.send("wishlist.item.added", event_payload)

        except Exception as e:
            logger.error(f"Manual add task: Failed to save item to DB for user {user_id}: {e}", exc_info=True)
            await kafka_producer.send("wishlist.item.add_failed",
                                      {"telegram_id": user_id, "source_url": item_request.item_url or item_request.name,
                                       "reason": f"Database error: {e}"})

    async def book_item(self, item_id: int, booker_user_id: int):
        item = await self.get_item_by_id(item_id)

        failure_payload = {
            "item_id": item_id,
            "booker_user_id": booker_user_id,
            "telegram_id": booker_user_id
        }

        if not item:
            logger.warning(f"Failed booking: Item {item_id} not found.")
            failure_payload["reason"] = "Item not found."
            await kafka_producer.send("wishlist.item.book_failed", failure_payload)
            return

        if item.wishlist.owner_user_id == booker_user_id:
            logger.warning(f"Failed booking: Owner {booker_user_id} cannot book their own item {item_id}.")
            failure_payload["reason"] = "You cannot book an item from your own wishlist."
            await kafka_producer.send("wishlist.item.book_failed", failure_payload)
            return

        existing_booking = next((b for b in item.bookings if b.booked_by_user_id == booker_user_id), None)
        if existing_booking:
            logger.warning(f"Failed booking: User {booker_user_id} has already booked item {item_id}.")
            failure_payload["reason"] = "You have already booked this item."
            await kafka_producer.send("wishlist.item.book_failed", failure_payload)
            return

        if not item.is_infinitely_bookable and item.bookings:
            logger.warning(
                f"Failed booking: Item {item_id} is already booked by someone else and is not infinitely bookable.")
            failure_payload["reason"] = "Item is already booked by someone else."
            await kafka_producer.send("wishlist.item.book_failed", failure_payload)
            return

        new_booking = ItemBooking(item_id=item_id, booked_by_user_id=booker_user_id)
        self.db_session.add(new_booking)
        await self.db_session.commit()
        await self.db_session.refresh(item)

        logger.info(f"Item {item_id} successfully booked by user {booker_user_id}")

        await kafka_producer.send("wishlist.item.booked", {
            "item_id": item_id,
            "item_name": item.name,
            "booked_by_user_id": booker_user_id,
            "owner_user_id": item.wishlist.owner_user_id,
            "telegram_id": booker_user_id,
            "is_infinitely_bookable": item.is_infinitely_bookable
        })

    async def unbook_item(self, item_id: int, unbooker_user_id: int):
        item = await self.get_item_by_id(item_id)

        failure_payload = {
            "item_id": item_id,
            "unbooker_user_id": unbooker_user_id,
            "telegram_id": unbooker_user_id
        }

        if not item:
            logger.warning(f"Failed unbooking: Item {item_id} not found.")
            failure_payload["reason"] = "Item not found."
            await kafka_producer.send("wishlist.item.unbook_failed", failure_payload)
            return

        if not item.bookings:
            logger.warning(f"Failed unbooking: Item {itemid} is not booked.")
            failure_payload["reason"] = "Item is not booked."
            await kafka_producer.send("wishlist.item.unbook_failed", failure_payload)
            return

        booking_to_delete = next((b for b in item.bookings if b.booked_by_user_id == unbooker_user_id), None)

        if not booking_to_delete:
            logger.warning(
                f"Failed unbooking: User {unbooker_user_id} cannot unbook item {item_id} which they haven't booked.")
            failure_payload["reason"] = "You can only unbook an item that you booked."
            await kafka_producer.send("wishlist.item.unbook_failed", failure_payload)
            return

        await self.db_session.delete(booking_to_delete)
        await self.db_session.commit()
        await self.db_session.refresh(item)

        logger.info(f"Item {item_id} successfully unbooked by user {unbooker_user_id}")

        await kafka_producer.send("wishlist.item.unbooked", {
            "item_id": item_id,
            "item_name": item.name,
            "unbooked_by_user_id": unbooker_user_id,
            "owner_user_id": item.wishlist.owner_user_id,
            "telegram_id": unbooker_user_id,
            "is_infinitely_bookable": item.is_infinitely_bookable
        })

    async def delete_item(self, item_id: int, deleter_user_id: int):
        item = await self.get_item_by_id(item_id)

        failure_payload = {
            "item_id": item_id,
            "deleter_user_id": deleter_user_id,
            "telegram_id": deleter_user_id
        }

        if not item:
            logger.warning(f"Failed deletion: Item {item_id} not found.")
            failure_payload["reason"] = "Item not found."
            await kafka_producer.send("wishlist.item.delete_failed", failure_payload)
            return

        if item.wishlist.owner_user_id != deleter_user_id:
            logger.warning(
                f"Failed deletion: User {deleter_user_id} cannot delete item {item_id} from wishlist of {item.wishlist.owner_user_id}.")
            failure_payload["reason"] = "You can only delete items from your own wishlist."
            await kafka_producer.send("wishlist.item.delete_failed", failure_payload)
            return

        item_name = item.name
        owner_user_id = item.wishlist.owner_user_id

        try:
            if item.bookings:
                logger.debug(f"Deleting {len(item.bookings)} associated booking(s) for item {item_id}")
                for booking in item.bookings:
                    await self.db_session.delete(booking)

            await self.db_session.delete(item)
            await self.db_session.commit()

            logger.info(f"Item {item_id} ({item_name}) successfully deleted by user {deleter_user_id}")

            await kafka_producer.send("wishlist.item.deleted", {
                "item_id": item_id,
                "item_name": item_name,
                "owner_user_id": owner_user_id,
                "telegram_id": deleter_user_id
            })

        except Exception as e:
            logger.error(f"Failed to delete item {item_id} from DB: {e}", exc_info=True)
            await self.db_session.rollback()
            failure_payload["reason"] = f"Database error: {e}"
            await kafka_producer.send("wishlist.item.delete_failed", failure_payload)

    async def get_and_push_wishlist_for_owner(self, owner_user_name: str, requester_user_id: int):
        wishlist = await self.get_wishlist_by_owner_id(owner_user_name)

        if not wishlist:
            logger.warning(f"No wishlist found for owner {owner_user_name} (requested by {requester_user_id}).")
            await kafka_producer.send("wishlist.view.owner_failed", {
                "owner_user_name": owner_user_name,
                "telegram_id": requester_user_id,
                "reason": "Wishlist not found."
            })
            return

        try:
            owner_schema = WishlistForOwner.model_validate(wishlist)
            owner_payload = owner_schema.model_dump(mode="json")
            owner_payload["telegram_id"] = requester_user_id
            await kafka_producer.send("wishlist.view.owner_success", owner_payload)
            logger.info(f"Sent owner view for wishlist {wishlist.wishlist_id} to user {requester_user_id}")
        except Exception as e:
            logger.error(f"Failed to serialize and send owner view for {owner_user_name}: {e}")
            await kafka_producer.send("wishlist.view.owner_failed", {
                "owner_user_name": owner_user_name,
                "telegram_id": requester_user_id,
                "reason": f"Internal serialization error: {e}"
            })

    async def get_and_push_wishlist_for_viewer(self, owner_user_name: str, requester_user_id: int):
        wishlist = await self.get_wishlist_by_owner_name(owner_user_name)

        if not wishlist:
            logger.warning(f"No wishlist found for owner {owner_user_name} (requested by {requester_user_id}).")
            await kafka_producer.send("wishlist.view.viewer_failed", {
                "owner_user_name": owner_user_name,
                "telegram_id": requester_user_id,
                "reason": "Wishlist not found."
            })
            return

        try:
            viewer_schema = WishlistForViewer.model_validate(wishlist)
            viewer_payload = viewer_schema.model_dump(mode="json")
            viewer_payload["telegram_id"] = requester_user_id
            await kafka_producer.send("wishlist.view.viewer_success", viewer_payload)
            logger.info(f"Sent viewer view for wishlist {wishlist.wishlist_id} to user {requester_user_id}")
        except Exception as e:
            logger.error(f"Failed to serialize and send viewer view for {owner_user_name}: {e}")
            await kafka_producer.send("wishlist.view.viewer_failed", {
                "owner_user_name": owner_user_name,
                "telegram_id": requester_user_id,
                "reason": f"Internal serialization error: {e}"
            })

    async def get_wishlist_by_owner_name(self, owner_user_name: str) -> Wishlist | None:
        logger.debug(f"Fetching wishlist by owner_user_id: {owner_user_name}")
        query = (
            select(Wishlist)
            .where(Wishlist.name == owner_user_name)
            .options(selectinload(Wishlist.items).selectinload(WishlistItem.bookings))
        )
        result = await self.db_session.execute(query)
        return result.scalar_one_or_none()