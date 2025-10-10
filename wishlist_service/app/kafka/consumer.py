import asyncio
import json
import logging
from aiokafka import AIOKafkaConsumer
from app.core.config import settings

logger = logging.getLogger(__name__)

class KafkaConsumer:
    def __init__(self, *topics: str):
        self.topics = topics
        self.consumer = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            loop=asyncio.get_event_loop(),
            group_id="my_group",
            auto_offset_reset='earliest',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        self._task = None

    async def start(self):
        logger.info(f"Starting KafkaConsumer for topics: {self.topics}")
        await self.consumer.start()
        self._task = asyncio.create_task(self._consume())

    async def stop(self):
        logger.info("Stopping KafkaConsumer...")
        if self._task:
            self._task.cancel()
        await self.consumer.stop()
        logger.info("KafkaConsumer stopped.")

    async def _consume(self):
        try:
            async for msg in self.consumer:
                logger.info(f"Consumed from {msg.topic}: key={msg.key} value={msg.value}")
        except asyncio.CancelledError:
            logger.info("Consumer task cancelled.")
        finally:
            logger.info("Consumer loop finished.")

# Пример создания инстанса для конкретных топиков
# consumer = KafkaConsumer("topic1", "topic2")