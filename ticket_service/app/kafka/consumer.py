import asyncio
import json
import logging
import tempfile
import os
from aiokafka import AIOKafkaConsumer
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.db import AsyncSessionLocal
from app.core.config import settings
from app.services.ticket_service import TicketService
from app.services.storage_service import storage_service
from app.kafka.producer import kafka_producer

logger = logging.getLogger(__name__)


class KafkaConsumer:
    def __init__(self, *topics: str):
        self.topics = topics
        self.consumer: AIOKafkaConsumer | None = None
        self._task = None

    async def start(self):
        logger.info(f"Starting KafkaConsumer for topics: {self.topics}")
        loop = asyncio.get_event_loop()
        self.consumer = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            loop=loop,
            group_id="ticket_service_group",
            auto_offset_reset='earliest',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        await self.consumer.start()
        self._task = loop.create_task(self._consume())
        logger.info("KafkaConsumer started.")

    async def stop(self):
        logger.info("Stopping KafkaConsumer...")
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self.consumer:
            await self.consumer.stop()
        logger.info("KafkaConsumer stopped.")

    async def _consume(self):
        try:
            async for msg in self.consumer:
                logger.info(f"Consumed from {msg.topic}: value={msg.value}")
                db: AsyncSession = AsyncSessionLocal()
                try:
                    logger.info(f"Handling event from topic {msg.topic}: {msg.value}")
                    if msg.topic == "ticket.create.requested":
                        await self.handle_create_ticket(db, msg.value)
                    elif msg.topic == "ticket.list.request":
                        await self.handle_list_request(db, msg.value)
                    elif msg.topic == "ticket.download.request":
                        await self.handle_download_request(db, msg.value)
                    elif msg.topic == "ticket.delete.request":
                        await self.handle_delete_request(db, msg.value)

                    await db.commit()
                except Exception as e:
                    logger.error(f"Error processing {msg.topic}: {e}", exc_info=True)
                    await db.rollback()
                finally:
                    await db.close()
        except asyncio.CancelledError:
            logger.info("Consumer task cancelled.")
        except Exception as e:
            logger.error(f"Kafka consumer error: {e}", exc_info=True)
        finally:
            logger.info("Consumer loop finished.")

    async def handle_create_ticket(self, db: AsyncSession, value: dict):
        title = value.get("title")
        user_id = value.get("telegram_id")
        object_name = value.get("ticket_file")

        if not all([title, user_id, object_name]):
            logger.warning(f"Invalid ticket.create.requested payload: {value}")
            return

        file_bytes = storage_service.download_file_as_bytes(object_name)
        if not file_bytes:
            logger.error(f"Failed to download {object_name} from MinIO.")
            return

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=True) as temp_file:
            temp_file.write(file_bytes)
            temp_path = temp_file.name
            logger.info(f"Downloading file {object_name} to temporary path {temp_path}")

            new_ticket = await TicketService.create_ticket(db, title, object_name, user_id, temp_path)

        logger.info(f"File {object_name} downloaded successfully to {temp_path}.")
        await kafka_producer.send("ticket.created",
                                  {"telegram_id": user_id, "ticket_id": new_ticket.id, "title": new_ticket.title})
        logger.info(f"Ticket {new_ticket.id} created for user {user_id}.")

    async def handle_list_request(self, db: AsyncSession, value: dict):
        telegram_id = value.get("telegram_id")
        admin_id = value.get("admin_id")

        if not telegram_id or not admin_id:
            logger.warning(f"Invalid ticket.list.request payload: {value}")
            return

        tickets = await TicketService.get_tickets_by_user(db, telegram_id)
        ticket_list = [t.to_schema().model_dump() for t in tickets]
        payload = {"telegram_id": telegram_id, "tickets": ticket_list}
        await kafka_producer.send("notification.send.tickets", payload)
        logger.info(f"Successfully sent ticket list for user {telegram_id}")

    async def handle_download_request(self, db: AsyncSession, value: dict):
        telegram_id = value.get("telegram_id")
        ticket_id = value.get("ticket_id")

        if not telegram_id or not ticket_id:
            logger.warning(f"Invalid ticket.download.request payload: {value}")
            return

        ticket = await TicketService.get_ticket_by_id(db, ticket_id, telegram_id)
        if not ticket:
            await kafka_producer.send("notification.send",
                                      {"telegram_id": telegram_id, "message": f"Билет {ticket_id} не найден."})
            return

        payload = {
            "telegram_id": telegram_id,
            "object_name": ticket.storage_key,
            "filename": f"{ticket.title.replace(' ', '_')}_{ticket.id}.pdf",
            "caption": f"Ваш билет: {ticket.title}"
        }
        await kafka_producer.send("notification.send.document", payload)
        logger.info(f"Sent download info for ticket {ticket_id} to user {telegram_id}")

    async def handle_delete_request(self, db: AsyncSession, value: dict):
        telegram_id = value.get("telegram_id")
        ticket_id = value.get("ticket_id")

        if not telegram_id or not ticket_id:
            logger.warning(f"Invalid ticket.delete.request payload: {value}")
            return

        ticket = await TicketService.get_ticket_by_id(db, ticket_id, telegram_id)
        if not ticket:
            await kafka_producer.send("notification.send",
                                      {"telegram_id": telegram_id, "message": f"Билет {ticket_id} не найден."})
            return

        try:
            storage_service.delete_file(ticket.storage_key)
            logger.info(f"Deleted {ticket.storage_key} from MinIO.")
        except Exception as e:
            logger.error(f"Failed to delete {ticket.storage_key} from MinIO: {e}")

        await TicketService.delete_ticket(db, ticket)
        logger.info(f"Deleted ticket {ticket_id} from DB.")
        await kafka_producer.send("notification.send",
                                  {"telegram_id": telegram_id, "message": f"Билет '{ticket.title}' удален."})


kafka_consumer = KafkaConsumer(
    "ticket.create.requested",
    "ticket.list.request",
    "ticket.download.request",
    "ticket.delete.request"
)