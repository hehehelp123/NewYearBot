import asyncio
import json
import logging
from aiokafka import AIOKafkaConsumer
from app.core.config import settings
from app.services.sync_service import music_sync_service
from app.schemas.music_schemas import YandexMusicSyncRequest

logger = logging.getLogger(__name__)


async def handle_sync_event(event_data: dict):
    try:
        sync_request = YandexMusicSyncRequest(**event_data)
        source_url_str = str(sync_request.source_url)

        if "music.yandex.ru" in source_url_str:
            await music_sync_service._sync_with_yandex_api(source_url_str)
        else:
            asyncio.create_task(music_sync_service._sync_with_selenium(source_url_str, sync_request.telegram_id))

        logger.info(f"Задача на синхронизацию принята для URL: {source_url_str}")

    except Exception as e:
        logger.error(f"Ошибка обработки события синхронизации: {e}")


class KafkaConsumer:
    def __init__(self, *topics: str):
        self.topics = topics
        self.consumer: AIOKafkaConsumer | None = None
        self._task = None

    async def start(self):
        self.consumer = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id="music_service_group",
            auto_offset_reset='earliest',
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            # Увеличиваем таймаут сессии до 5 минут
            session_timeout_ms=300000,
            heartbeat_interval_ms=10000
        )
        await self.consumer.start()
        self._task = asyncio.create_task(self._consume())
        logger.info(f"Music Service KafkaConsumer запущен для топиков: {self.topics}")

    async def stop(self):
        if self._task:
            self._task.cancel()
        if self.consumer:
            await self.consumer.stop()
        logger.info("Music Service KafkaConsumer остановлен.")

    async def _consume(self):
        if not self.consumer: return
        try:
            async for msg in self.consumer:
                logger.info(f"Получена команда из {msg.topic}: value={msg.value}")
                if msg.topic == "music.sync.start":
                    await handle_sync_event(msg.value)
        except asyncio.CancelledError:
            logger.info("Задача консумера отменена.")
        finally:
            logger.info("Цикл консумера завершен.")