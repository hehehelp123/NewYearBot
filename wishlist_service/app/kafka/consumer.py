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
                "source_url": wishlist_request.source_text,
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
        try:
            await kafka_producer.send("wishlist.item.add_failed", {
                "telegram_id": event_data.get("telegram_id", "unknown"),
                "source_url": event_data.get("source_text", "unknown"),
                "reason": f"Internal error during task scheduling: {e}"
            })
        except Exception as kafka_e:
            logger.error(f"Failed to send add_failed event after scheduling error: {kafka_e}")


async def handle_wishlist_create_event(event_data: dict):
    try:
        wishlist_create_data = WishlistCreate(
            owner_user_id=event_data["telegram_id"],
            name=event_data["username"]
        )
        async with AsyncSessionLocal() as session:
            try:
                service = WishlistService(session)
                await service.create_wishlist(wishlist_create_data)
            except Exception as e:
                logger.error(f"Error processing wishlist create event in service: {e}", exc_info=True)

        logger.info(f"Task to create Wishlist for user_id: {wishlist_create_data.owner_user_id} accepted.")

    except Exception as e:
        logger.error(f"Error processing wishlist create event handler: {e}", exc_info=True)


async def handle_wishlist_add_manual_event(event_data: dict):
    try:
        item_request = ItemManualAddRequest(**event_data)

        async with AsyncSessionLocal() as session:
            service = WishlistService(session)
            await service.add_manual_item(item_request)

        logger.info(f"Task to manually add item {item_request.name} for user {item_request.telegram_id} processed.")

    except Exception as e:
        logger.error(f"Error processing manual add event: {e}", exc_info=True)
        try:
            await kafka_producer.send("wishlist.item.add_failed", {
                "telegram_id": event_data.get("telegram_id", "unknown"),
                "source_url": event_data.get("item_url") or event_data.get("name", "unknown"),
                "reason": f"Internal error during manual add: {e}"
            })
        except Exception as kafka_e:
            logger.error(f"Failed to send add_failed event after manual add error: {kafka_e}")


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
        try:
            await kafka_producer.send("wishlist.item.book_failed", {
                "item_id": event_data.get("item_id", "unknown"),
                "booker_user_id": event_data.get("booker_user_id", "unknown"),
                "telegram_id": event_data.get("booker_user_id", "unknown"),
                "reason": f"Internal error during booking: {e}"
            })
        except Exception as kafka_e:
            logger.error(f"Failed to send book_failed event after booking error: {kafka_e}")


async def handle_wishlist_unbook_event(event_data: dict):
    try:
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
        try:
            await kafka_producer.send("wishlist.item.unbook_failed", {
                "item_id": event_data.get("item_id", "unknown"),
                "unbooker_user_id": event_data.get("unbooker_user_id", "unknown"),
                "telegram_id": event_data.get("unbooker_user_id", "unknown"),
                "reason": f"Internal error during unbooking: {e}"
            })
        except Exception as kafka_e:
            logger.error(f"Failed to send unbook_failed event after unbooking error: {kafka_e}")


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
        try:
            await kafka_producer.send("wishlist.item.delete_failed", {
                "item_id": event_data.get("item_id", "unknown"),
                "deleter_user_id": event_data.get("deleter_user_id", "unknown"),
                "telegram_id": event_data.get("deleter_user_id", "unknown"),
                "reason": f"Internal error during deletion: {e}"
            })
        except Exception as kafka_e:
            logger.error(f"Failed to send delete_failed event after deletion error: {kafka_e}")


async def handle_wishlist_get_owner(event_data: dict):
    try:
        request = WishlistGetRequest(
            owner_user_id=event_data.get("owner_user_id"),  # Owner ID is needed here
            requester_user_id=event_data["telegram_id"]
        )
        if not request.owner_user_id:
            raise ValueError("'owner_user_id' not found in event data for get_owner")

        async with AsyncSessionLocal() as session:
            service = WishlistService(session)
            await service.get_and_push_wishlist_for_owner(request.owner_user_id, request.requester_user_id)
        logger.info(
            f"Task to get owner view for {request.owner_user_id} (requested by {request.requester_user_id}) processed.")
    except Exception as e:
        logger.error(f"Error processing get_owner event: {e}", exc_info=True)
        try:
            await kafka_producer.send("wishlist.view.owner_failed", {
                "owner_user_id": event_data.get("owner_user_id", "unknown"),
                "telegram_id": event_data.get("telegram_id", "unknown"),
                "reason": f"Internal error processing get_owner: {e}"
            })
        except Exception as kafka_e:
            logger.error(f"Failed to send owner_failed event after get_owner error: {kafka_e}")


async def handle_wishlist_get_viewer(event_data: dict):
    try:
        request = WishlistGetRequest(
            owner_user_name=event_data.get("target_user", event_data.get("owner_user_name")),
            requester_user_id=event_data["telegram_id"]
        )
        if not request.owner_user_name:
            raise ValueError("'owner_user_name' or 'target_user' not found in event data for get_viewer")

        async with AsyncSessionLocal() as session:
            service = WishlistService(session)
            await service.get_and_push_wishlist_for_viewer(request.owner_user_name, request.requester_user_id)
        logger.info(
            f"Task to get viewer view for {request.owner_user_name} (requested by {request.requester_user_id}) processed.")
    except Exception as e:
        logger.error(f"Error processing get_viewer event: {e}", exc_info=True)
        try:
            await kafka_producer.send("wishlist.view.viewer_failed", {
                "owner_user_name": event_data.get("target_user", event_data.get("owner_user_name", "unknown")),
                "telegram_id": event_data.get("telegram_id", "unknown"),
                "reason": f"Internal error processing get_viewer: {e}"
            })
        except Exception as kafka_e:
            logger.error(f"Failed to send viewer_failed event after get_viewer error: {kafka_e}")


async def handle_wishlist_get_booked(event_data: dict):
    try:
        requester_user_id = event_data["telegram_id"]
        logger.warning(f"Handler 'get_booked' is not implemented. Skipping for user {requester_user_id}.")
        await kafka_producer.send("wishlist.view.booked_items_failed", {
            "telegram_id": requester_user_id,
            "reason": "This feature is not yet implemented."
        })
    except Exception as e:
        logger.error(f"Error processing get_booked event: {e}", exc_info=True)


async def handle_wishlist_get_all(event_data: dict):
    try:
        requester_user_id = event_data["telegram_id"]
        async with AsyncSessionLocal() as session:
            service = WishlistService(session)
            await service.get_and_push_all_wishlist_owners(requester_user_id)
        logger.info(f"Task to get all wishlist owners for user {requester_user_id} processed.")
    except Exception as e:
        logger.error(f"Error processing get_all event: {e}", exc_info=True)
        try:
            await kafka_producer.send("wishlist.view.all_failed", {
                "telegram_id": event_data.get("telegram_id", "unknown"),
                "reason": f"Internal error processing get_all: {e}"
            })
        except Exception as kafka_e:
            logger.error(f"Failed to send all_failed event after get_all error: {kafka_e}")


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
            try:
                await self.task
            except asyncio.CancelledError:
                logger.info("Consumer task cancelled.")
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
                elif msg.topic == "wishlist.view.owner":
                    await handle_wishlist_get_owner(msg.value)
                elif msg.topic == "wishlist.view.viewer":
                    await handle_wishlist_get_viewer(msg.value)
                elif msg.topic == "wishlist.view.booked_items":
                    await handle_wishlist_get_booked(msg.value)
                elif msg.topic == "wishlist.view.all":
                    await handle_wishlist_get_all(msg.value)
        except asyncio.CancelledError:
            logger.info("Задача консумера отменена.")
        except Exception as e:
            logger.error(f"Critical error in consumer loop: {e}", exc_info=True)
        finally:
            logger.info("Цикл консумера завершен.")