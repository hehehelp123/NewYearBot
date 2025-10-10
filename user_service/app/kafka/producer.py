import asyncio
import json
import logging
from aiokafka import AIOKafkaProducer
from app.core.config import settings

logger = logging.getLogger(__name__)


class KafkaProducer:
    def __init__(self):
        self.producer: AIOKafkaProducer | None = None
        self._is_running = False

    async def start(self):
        if not self._is_running:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            await self.producer.start()
            self._is_running = True
            logger.info("KafkaProducer started.")

    async def stop(self):
        if self._is_running and self.producer:
            await self.producer.stop()
            self._is_running = False
            logger.info("KafkaProducer stopped.")

    async def send(self, topic: str, value: dict):
        if not self._is_running or not self.producer:
            await self.start()

        if self.producer:
            try:
                await self.producer.send_and_wait(topic, value)
            except Exception as e:
                logger.error(f"Failed to send message to Kafka topic {topic}: {e}")


kafka_producer = KafkaProducer()