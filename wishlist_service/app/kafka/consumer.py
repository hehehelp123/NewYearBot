import asyncio
import json
import logging
import re
from aiokafka import AIOKafkaConsumer
from app.core.config import settings
from app.schemas.wishlist_schemas import (
    WishlistAddRequest,
    WishlistCreate,
    ItemBookRequest,
    ItemDeleteRequest,
    WishlistGetRequest,
    ItemManualAddRequest
)
from app.core.db import AsyncSessionLocal
from app.services.wishlist_service import WishlistService
from app.kafka.producer import kafka_producer

logger = logging.getLogger(__name__)


async def handle_wishlist_add_event(event_data: dict):
    try:
        wishlist_request = WishlistAddRequest(**event_data)

        url_match = re.search(r'https?://[^\s/$.?#].[^\s]*', wishlist_request.source_text)

        if not url_match:
            logger.warning(
                f"No URL found in message from {wishlist_request.telegram_id}: {wishlist_request.source_text}")
            await kafka_producer.send("wishlist.item.add_failed", {
                "telegram_id": wishlist_request.telegram_id,
                "source_url": wishlist_request.source_text,  # Keep original text for context
                "reason": "No valid URL was found in your message."
            })
            return

        source_url_str = url_match.group(0)
        is_infinitely_bookable = wishlist_request.is_infinitely_bookable

        async with AsyncSessionLocal() as session:
            asyncio.create_task(
                WishlistService(session).add_to_wishlist(
                    source_url_str,
                    wishlist_request.telegram_id,
                    is_infinitely_bookable
                )
            )

        logger.info(f"Scraping task for {source_url_str} has been scheduled.")

    except Exception as e:
        logger.error(f"Error scheduling scraping task: {e}", exc_info=True)


async def handle_wishlist_create_event(event_data: dict):
    try:
        # Assuming event_data contains 'telegram_id' and 'username'
        wishlist_create_data = WishlistCreate(
            owner_user_id=event_data["telegram_id"],
            name=event_data["username"]  # Use username from event
        )
        async with AsyncSessionLocal() as session:
            try:
                service = WishlistService(session)
                await service.create_wishlist(wishlist_create_data)
            except Exception as e:
                logger.error(f"Error processing wishlist create event: {e}")

        logger.info(f"Task to create Wishlist for user_id: {wishlist_create_data.owner_user_id} accepted.")

    except Exception as e:
        logger.error(f"Error processing wishlist create event: {e}", exc_info=True)


async def handle_wishlist_add_manual_event(event_data: dict):
    try:
        item_request = ItemManualAddRequest(**event_data)

        async with AsyncSessionLocal() as session:
            service = WishlistService(session)
            await service.add_manual_item(item_request)

        logger.info(f"Task to manually add item {item_request.name} for user {item_request.telegram_id} processed.")

    except Exception as e:
        logger.error(f"Error processing manual add event: {e}", exc_info=True)


async def handle_wishlist_book_event(event_data: dict):
    try:
        book_request = ItemBookRequest(
            item_id=event_data["item_id"],
            booker_user_id=event_data["booker_user_id"]
        )

        async with AsyncSessionLocal() as session:
            service = WishlistService(session)
            await service.book_item(book_request.item_id, book_request.booker_user_id)

        logger.info(f"Task to book item {book_request.item_id} for user {book_request.booker_user_id} processed.")

    except Exception as e:
        logger.error(f"Error processing book event: {e}", exc_info=True)


async def handle_wishlist_unbook_event(event_data: dict):
    try:
        # Pydantic schema expects 'booker_user_id', but Kafka event has 'unbooker_user_id'
        unbook_request = ItemBookRequest(
            item_id=event_data["item_id"],
            booker_user_id=event_data["unbooker_user_id"]
        )

        async with AsyncSessionLocal() as session:
            service = WishlistService(session)
            await service.unbook_item(unbook_request.item_id, unbook_request.booker_user_id)

        logger.info(f"Task to unbook item {unbook_request.item_id} for user {unbook_request.booker_user_id} processed.")

    except Exception as e:
        logger.error(f"Error processing unbook event: {e}", exc_info=True)


async def handle_wishlist_delete_event(event_data: dict):
    try:
        delete_request = ItemDeleteRequest(
            item_id=event_data["item_id"],
            deleter_user_id=event_data["deleter_user_id"]
        )

        async with AsyncSessionLocal() as session:
            service = WishlistService(session)
            await service.delete_item(delete_request.item_id, delete_request.deleter_user_id)

        logger.info(f"Task to delete item {delete_request.item_id} by user {delete_request.deleter_user_id} processed.")

    except Exception as e:
        logger.error(f"Error processing delete event: {e}", exc_info=True)


async def handle_wishlist_get_owner(event_data: dict):
    try:
        # Handle potential key difference ('target_user' vs 'owner_user_name')
        request = WishlistGetRequest(
            owner_user_name=event_data.get("target_user", event_data.get("owner_user_name")),
            requester_user_id=event_data["telegram_id"]
        )
        async with AsyncSessionLocal() as session:
            service = WishlistService(session)
            await service.get_and_push_wishlist_for_owner(request.owner_user_name, request.requester_user_id)
        logger.info(
            f"Task to get owner view for {request.owner_user_name} (requested by {request.requester_user_id}) processed.")
    except Exception as e:
        logger.error(f"Error processing get_owner event: {e}", exc_info=True)


async def handle_wishlist_get_viewer(event_data: dict):
    try:
        # Handle potential key difference ('target_user' vs 'owner_user_name')
        request = WishlistGetRequest(
            owner_user_name=event_data.get("target_user", event_data.get("owner_user_name")),
            requester_user_id=event_data["telegram_id"]
        )
        async with AsyncSessionLocal() as session:
            service = WishlistService(session)
            await service.get_and_push_wishlist_for_viewer(request.owner_user_name, request.requester_user_id)
        logger.info(
            f"Task to get viewer view for {request.owner_user_name} (requested by {request.requester_user_id}) processed.")
    except Exception as e:
        logger.error(f"Error processing get_viewer event: {e}", exc_info=True)


async def handle_wishlist_get_booked(event_data: dict):
    try:
        requester_user_id = event_data["telegram_id"]
        async with AsyncSessionLocal() as session:
            service = WishlistService(session)
            # await service.get_and_push_booked_items_for_user(requester_user_id) # Ensure this method exists
            logger.warning(
                f"Handler 'get_booked' might not be fully implemented in WishlistService for user {requester_user_id}.")
        logger.info(f"Task to get booked items for user {requester_user_id} processed.")
    except Exception as e:
        logger.error(f"Error processing get_booked event: {e}", exc_info=True)


async def handle_wishlist_get_all(event_data: dict):
    try:
        requester_user_id = event_data["telegram_id"]
        async with AsyncSessionLocal() as session:
            service = WishlistService(session)
            # await service.get_and_push_all_wishlist_owners(requester_user_id) # Ensure this method exists
            logger.warning(
                f"Handler 'get_all' might not be fully implemented in WishlistService for user {requester_user_id}.")
        logger.info(f"Task to get all wishlist owners for user {requester_user_id} processed.")
    except Exception as e:
        logger.error(f"Error processing get_all event: {e}", exc_info=True)


class KafkaConsumer:
    def __init__(self, *topics: str):
        self.topics = topics
        self.consumer: AIOKafkaConsumer | None = None
        self._task = None

    async def start(self):
        self.consumer = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id="wishlist_service_group",
            auto_offset_reset='earliest',
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            session_timeout_ms=300000,
            heartbeat_interval_ms=10000
        )
        await self.consumer.start()
        self.task = asyncio.create_task(self._consume())
        logger.info(f"Consumer запущен для топиков: {self.topics}")

    async def stop(self):
        if self.task:
            self.task.cancel()
        if self.consumer:
            await self.consumer.stop()
        logger.info("Consumer остановлен.")

    async def _consume(self):
        if not self.consumer: return
        try:
            async for msg in self.consumer:
                logger.info(f"Получена команда из {msg.topic}: value={msg.value}")
                if msg.topic == "wishlist.wishlist.add":
                    await handle_wishlist_add_event(msg.value)
                elif msg.topic == "wishlist.wishlist.create":
                    await handle_wishlist_create_event(msg.value)
                elif msg.topic == "wishlist.item.add_manual":
                    await handle_wishlist_add_manual_event(msg.value)
                elif msg.topic == "wishlist.item.book":
                    await handle_wishlist_book_event(msg.value)
                elif msg.topic == "wishlist.item.unbook":
                    await handle_wishlist_unbook_event(msg.value)
                elif msg.topic == "wishlist.item.delete":
                    await handle_wishlist_delete_event(msg.value)
                elif msg.topic == "wishlist.view.owner":  # Added missing topic
                    await handle_wishlist_get_owner(msg.value)
                elif msg.topic == "wishlist.view.viewer":
                    await handle_wishlist_get_viewer(msg.value)
                elif msg.topic == "wishlist.view.booked_items":
                    await handle_wishlist_get_booked(msg.value)
                elif msg.topic == "wishlist.view.all":
                    await handle_wishlist_get_all(msg.value)
        except asyncio.CancelledError:
            logger.info("Задача консумера отменена.")
        finally:
            logger.info("Цикл консумера завершен.")