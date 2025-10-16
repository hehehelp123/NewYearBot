from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.api.v1.routers import router
from app.kafka.consumer import KafkaTicketConsumer
from app.kafka.producer import kafka_producer

@asynccontextmanager
async def lifespan(app: FastAPI):
    await kafka_producer.start()
    consumer = KafkaTicketConsumer(
        "ticket.create.requested",
        "ticket.list.request",
        "ticket.download.request",
        "ticket.delete.request"
    )
    await consumer.start()
    yield
    await kafka_producer.stop()
    await consumer.stop()

app = FastAPI(title="Ticket Service", lifespan=lifespan)
app.include_router(router, prefix="/api/v1")