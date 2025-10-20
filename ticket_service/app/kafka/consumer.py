import asyncio
import json
import logging
from aiokafka import AIOKafkaConsumer
from app.core.config import settings
from app.core.db import AsyncSessionLocal
from app.services.ticket_service import TicketService

logger = logging.getLogger(__name__)

async def handle_event(event_data: dict, topic: str):
    logger.info(f"Handling event from topic {topic}: {event_data}")
    async with AsyncSessionLocal() as session:
        service = TicketService(session)
        user_id = event_data.get("telegram_id")
        ticket_id = event_data.get("ticket_id")

        if topic == "ticket.create.requested":
            await service.process_ticket_creation_request(event_data)
        elif topic == "ticket.list.request":
            await service.process_ticket_list_request(event_data)
        elif topic == "ticket.download.request" and user_id and ticket_id:
            await service.send_ticket_document(user_id, ticket_id)
        elif topic == "ticket.delete.request" and user_id and ticket_id:
            await service.delete_ticket_by_id(user_id, ticket_id)

class KafkaTicketConsumer:
    def __init__(self, *topics: str):
        self.topics = topics
        self.consumer = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id="ticket_service_group",
            auto_offset_reset='earliest',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        self._task = None

    async def start(self):
        logger.info(f"Starting KafkaTicketConsumer for topics: {self.topics}")
        await self.consumer.start()
        self._task = asyncio.create_task(self._consume())

    async def stop(self):
        logger.info("Stopping KafkaTicketConsumer...")
        if self._task:
            self._task.cancel()
        await self.consumer.stop()
        logger.info("KafkaTicketConsumer stopped.")

    async def _consume(self):
        try:
            async for msg in self.consumer:
                try:
                    logger.info(f"Consumed from {msg.topic}: value={msg.value}")
                    await handle_event(msg.value, msg.topic)
                except Exception as e:
                    logger.error(f"Failed to process message: {e}", exc_info=True)
        except asyncio.CancelledError:
            logger.info("Consumer task cancelled.")
        finally:
            logger.info("Consumer loop finished.")