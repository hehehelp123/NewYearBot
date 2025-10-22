import asyncio
import logging
from typing import AsyncGenerator
from contextlib import asynccontextmanager

from aiogram import Bot, Dispatcher, F
from aiogram.filters import StateFilter # <-- Import StateFilter
from fastapi import FastAPI

from app.core.config import settings
from app.core.http_client import http_client
from app.kafka.consumer import KafkaBotConsumer
from app.kafka.producer import kafka_producer
from app.bot.bot_app import (
    START_BUTTON,
    ActionForm,
    WishlistBrowser,  # <-- Import new state
    callback_query_handler,
    menu_handler,
    process_action_field,
    start_button_handler,
    start_handler,
    wishlist_navigation_handler, # <-- Import new handler
)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    logging.basicConfig(level=logging.INFO)
    await http_client.start()

    bot = Bot(token=settings.BOT_TOKEN)
    app.state.bot = bot
    dp = Dispatcher()

    await kafka_producer.start()

    topics_to_consume = [
        "notification.send",
        "notification.send.document",
        "notification.send.tickets",
        "wishlist.view.owner",
        "wishlist.view.viewer",
        "wishlist.view.owner_failed",
        "wishlist.view.viewer_failed",
    ]

    kafka_consumer = KafkaBotConsumer(bot, dp, *topics_to_consume)
    await kafka_consumer.start()

    logging.info("Starting aiogram bot polling...")

    dp.callback_query.register(
        wishlist_navigation_handler,
        StateFilter(WishlistBrowser.browsing)
    )

    dp.message.register(start_button_handler, F.text == START_BUTTON)
    dp.message.register(start_handler, F.text == "/start")
    dp.message.register(process_action_field, StateFilter(ActionForm.waiting_for_field), F.text | F.document)
    dp.callback_query.register(callback_query_handler)
    dp.message.register(menu_handler, F.text)

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
            pass
        await bot.session.close()
        logging.info("Aiogram bot shutdown complete")