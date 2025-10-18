import asyncio
import json
import logging
from aiokafka import AIOKafkaConsumer
from app.core.config import settings
from app.services.sync_service import music_sync_service

logger = logging.getLogger(__name__)

class MusicKafkaConsumer:
    def __init__(self, *topics: str):
        self.topics = topics
        self.consumer = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id="music_uploader_group",
            auto_offset_reset='earliest',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        self._task = None

    async def start(self):
        logger.info(f"Starting Music Service KafkaConsumer for topics: {self.topics}")
        await self.consumer.start()
        self._task = asyncio.create_task(self._consume())

    async def stop(self):
        logger.info("Stopping Music Service KafkaConsumer...")
        if self._task:
            self._task.cancel()
        await self.consumer.stop()
        logger.info("Music Service KafkaConsumer stopped.")

    async def _consume(self):
        try:
            async for msg in self.consumer:
                try:
                    logger.info(f"Consumed command from topic '{msg.topic}': value={msg.value}")
                    await music_sync_service.process_sync_request(msg.value)
                except Exception as e:
                    logger.error(f"Failed to process message from '{msg.topic}': {e}", exc_info=True)
        except asyncio.CancelledError:
            logger.info("Music Service consumer task cancelled.")
        finally:
            logger.info("Music Service consumer loop finished.")

music_kafka_consumer = MusicKafkaConsumer("music.sync.start")