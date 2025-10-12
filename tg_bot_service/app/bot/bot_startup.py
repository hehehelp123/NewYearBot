import asyncio
import logging
from typing import AsyncGenerator
from app.core.config import settings

from aiogram import Bot, Dispatcher
from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.core.http_client import http_client
from app.kafka.producer import kafka_producer
from app.kafka.consumer import KafkaBotConsumer  # Импортируем наш консумер

from app.bot.bot_app import start_button_handler, start_handler, process_action_field, menu_handler, ActionForm, \
    START_BUTTON, F


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    logging.basicConfig(level=logging.INFO)
    await http_client.start()

    bot = Bot(token=settings.BOT_TOKEN)
    app.state.bot = bot
    dp = Dispatcher()

    # Запускаем продюсер и консумер Kafka
    await kafka_producer.start()
    kafka_consumer = KafkaBotConsumer(bot, "notification.send")
    await kafka_consumer.start()

    logging.info("Starting aiogram bot polling...")

    dp.message.register(start_button_handler, F.text == START_BUTTON)
    dp.message.register(start_handler, F.text == "/start")
    dp.message.register(process_action_field, ActionForm.waiting_for_field)
    dp.message.register(menu_handler, F.text)

    polling_task = asyncio.create_task(dp.start_polling(bot))
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