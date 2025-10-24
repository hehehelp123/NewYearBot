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
from app.bot.bot_app import (
    START_BUTTON,
    BACK_TO_WELCOME_BUTTON,
    UPLOAD_MEDIA_BUTTON,
    VIEW_ALBUMS_BUTTON,
    STOP_UPLOAD_BUTTON,
    ActionForm,
    WishlistBrowser,
    MediaUpload,
    AlbumBrowser,
    callback_query_handler,
    menu_handler,
    process_action_field,
    start_button_handler,
    start_handler,
    wishlist_navigation_handler,
    show_main_menu_callback,
    show_info_callback,
    back_to_welcome_handler,
    start_media_upload_handler,
    stop_media_upload_handler,
    process_media_year_handler,
    media_upload_handler,
    start_album_view_handler,
    album_navigation_handler
)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    logging.basicConfig(level=logging.INFO)
    logging.info("Application lifespan start")
    await http_client.start()

    bot = Bot(token=settings.BOT_TOKEN)
    app.state.bot = bot
    dp = Dispatcher()

    await kafka_producer.start()

    topics_to_consume = [
        "notification.send",
        "notification.send.document",
        "notification.send.tickets",
        "wishlist.view.owner_success",
        "wishlist.view.viewer_success",
        "wishlist.view.owner_failed",
        "wishlist.view.viewer_failed",
        "album.send.media",
    ]

    kafka_consumer = KafkaBotConsumer(bot, dp, *topics_to_consume)
    await kafka_consumer.start()

    logging.info("Starting aiogram bot registration...")

    dp.callback_query.register(
        wishlist_navigation_handler,
        StateFilter(WishlistBrowser.browsing)
    )
    dp.callback_query.register(
        album_navigation_handler,
        StateFilter(AlbumBrowser)
    )

    dp.message.register(
        process_action_field,
        StateFilter(ActionForm.waiting_for_field),
        F.text | F.document
    )
    dp.message.register(
        stop_media_upload_handler,
        StateFilter(MediaUpload.uploading),
        F.text == STOP_UPLOAD_BUTTON
    )
    dp.message.register(
        media_upload_handler,
        StateFilter(MediaUpload.uploading),
        F.photo | F.video
    )
    dp.message.register(
        process_media_year_handler,
        StateFilter(MediaUpload.waiting_for_year),
        F.text
    )

    dp.message.register(start_handler, F.text == "/start")
    dp.message.register(start_button_handler, F.text == START_BUTTON)
    dp.message.register(
        back_to_welcome_handler,
        F.text == BACK_TO_WELCOME_BUTTON
    )
    dp.message.register(
        start_media_upload_handler,
        F.text == UPLOAD_MEDIA_BUTTON
    )
    dp.message.register(
        start_album_view_handler,
        F.text == VIEW_ALBUMS_BUTTON
    )

    dp.callback_query.register(
        show_main_menu_callback,
        F.data == "info:go_to_main_menu"
    )
    dp.callback_query.register(
        show_info_callback,
        F.data.startswith("info:")
    )
    dp.callback_query.register(
        callback_query_handler,
        F.data.startswith(("download_ticket:", "delete_ticket:", "confirm_delete:", "cancel_delete:"))
    )

    dp.message.register(menu_handler, F.text)

    logging.info("Handlers registered. Starting polling...")

    polling_task = asyncio.create_task(dp.start_polling(bot, drop_pending_updates=True))
    logging.info("Aiogram bot polling started")

    try:
        yield
    finally:
        await http_client.stop()
        await kafka_producer.stop()
        await kafka_consumer.stop()
        logging.info("Shutting down aiogram bot...")
        polling_task.cancel()
        try:
            await polling_task
        except asyncio.CancelledError:
            logging.info("Polling task cancelled")
        await bot.session.close()
        logging.info("Aiogram bot shutdown complete")