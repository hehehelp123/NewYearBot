import asyncio
import logging
from typing import AsyncGenerator
from contextlib import asynccontextmanager

from aiogram import Bot, Dispatcher, F
from aiogram.filters import StateFilter
from fastapi import FastAPI

from app.core.config import settings
from app.core.http_client import http_client
from app.kafka.consumer import KafkaBotConsumer
from app.kafka.producer import kafka_producer
from app.middlewares.access_middleware import AccessMiddleware

from app.bot.constants import (
    START_BUTTON, BACK_TO_WELCOME_BUTTON, UPLOAD_MEDIA_BUTTON, VIEW_ALBUMS_BUTTON,
    STOP_UPLOAD_BUTTON
)
from app.bot.states import (
    ActionForm, AllWishlistsBrowser, WishlistBrowser, MediaUpload, AlbumBrowser,
    UserRemoval, WishlistAddManual
)

from app.handlers.common import (
    start_button_handler, start_handler, back_to_welcome_handler,
    show_main_menu_callback, show_info_callback
)
from app.handlers.menu import menu_handler, process_action_field
from app.handlers.media import (
    start_media_upload_handler, stop_media_upload_handler,
    process_media_year_handler, media_upload_handler
)
from app.handlers.albums import start_album_view_handler, album_navigation_handler
from app.handlers.admin import (
    handle_remove_user_confirm, handle_remove_user_delete, handle_remove_user_cancel
)
from app.handlers.tickets import tickets_callback_handler
from app.handlers.wishlist import (
    all_wishlists_navigation_handler, wishlist_navigation_handler,
    wishlist_add_url_handler, process_manual_wishlist_name,
    process_manual_wishlist_cost, process_manual_wishlist_url,
    process_manual_wishlist_delivery, process_manual_wishlist_confirm
)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    logging.basicConfig(level=logging.INFO); logging.info("Lifespan start")
    await http_client.start(); bot = Bot(token=settings.BOT_TOKEN); app.state.bot = bot
    dp = Dispatcher()

    dp.update.outer_middleware(AccessMiddleware())

    await kafka_producer.start()
    topics_to_consume = [
        "notification.send", "notification.send.document", "notification.send.tickets",
        "wishlist.view.owner_success", "wishlist.view.viewer_success",
        "wishlist.view.owner_failed", "wishlist.view.viewer_failed",
        "user.user.allowed", "user.user.disallowed", "user.user.list_response",
        "wishlist.view.all_success",
        "wishlist.item.added",
        "wishlist.item.add_failed",
        "wishlist.item.parse_failed",
        "wishlist.item.booked",
        "wishlist.item.unbooked",
        "wishlist.item.book_failed",
        "wishlist.item.unbook_failed",
        "wishlist.item.deleted",
        "wishlist.item.delete_failed"
    ]
    kafka_consumer = KafkaBotConsumer(bot, dp, *topics_to_consume)
    await kafka_consumer.start()
    logging.info("Starting handler registration...")

    dp.message.register(start_button_handler, F.text == START_BUTTON, StateFilter("*"))

    dp.callback_query.register(wishlist_navigation_handler, StateFilter(WishlistBrowser.browsing))
    dp.callback_query.register(album_navigation_handler, StateFilter(AlbumBrowser))
    dp.callback_query.register(handle_remove_user_confirm, StateFilter(UserRemoval.choosing_user), F.data.startswith("remove_user_confirm:"))
    dp.callback_query.register(handle_remove_user_delete, StateFilter(UserRemoval.confirming_delete), F.data.startswith("remove_user_delete:"))
    dp.callback_query.register(handle_remove_user_cancel, StateFilter(UserRemoval), F.data == "remove_user_cancel")

    dp.message.register(process_action_field, StateFilter(ActionForm.waiting_for_field), F.text | F.document)
    dp.message.register(stop_media_upload_handler, StateFilter(MediaUpload.uploading), F.text == STOP_UPLOAD_BUTTON)
    dp.message.register(media_upload_handler, StateFilter(MediaUpload.uploading), F.photo | F.video)
    dp.message.register(process_media_year_handler, StateFilter(MediaUpload.waiting_for_year), F.text)

    dp.message.register(process_manual_wishlist_name, StateFilter(WishlistAddManual.waiting_for_name), F.text)
    dp.message.register(process_manual_wishlist_cost, StateFilter(WishlistAddManual.waiting_for_cost), F.text)
    dp.message.register(process_manual_wishlist_url, StateFilter(WishlistAddManual.waiting_for_url), F.text)
    dp.message.register(process_manual_wishlist_delivery, StateFilter(WishlistAddManual.waiting_for_delivery), F.text)
    dp.callback_query.register(process_manual_wishlist_confirm, StateFilter(WishlistAddManual.confirming), F.data.startswith("wishlist_manual_confirm:"))

    dp.message.register(start_handler, F.text == "/start")
    dp.message.register(back_to_welcome_handler, F.text == BACK_TO_WELCOME_BUTTON)

    dp.callback_query.register(all_wishlists_navigation_handler, StateFilter(AllWishlistsBrowser.choosing_owner))
    dp.callback_query.register(show_main_menu_callback, F.data == "info:go_to_main_menu")
    dp.callback_query.register(show_info_callback, F.data.startswith("info:"))
    dp.callback_query.register(wishlist_add_url_handler, F.data.startswith("wishlist_add_url:"))
    dp.callback_query.register(tickets_callback_handler, F.data.startswith(("download_ticket:", "delete_ticket:", "confirm_delete:", "cancel_delete:")))

    dp.message.register(menu_handler, F.text)

    logging.info("Handlers registered. Starting polling...")
    polling_task = asyncio.create_task(dp.start_polling(bot, drop_pending_updates=True))
    logging.info("Aiogram bot polling started")
    try: yield
    finally:
        await http_client.stop(); await kafka_producer.stop(); await kafka_consumer.stop()
        logging.info("Shutting down polling..."); polling_task.cancel()
        try: await polling_task
        except asyncio.CancelledError: logging.info("Polling cancelled")
        await bot.session.close(); logging.info("Shutdown complete")