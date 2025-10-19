from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.api.v1.routers import router
from app.kafka.consumer import KafkaNotificationConsumer
from app.kafka.producer import kafka_producer
from app.services.scheduler import start_scheduler

@asynccontextmanager
async def lifespan(app: FastAPI):
    await kafka_producer.start()
    consumer = KafkaNotificationConsumer(
        "user.user.created",
        "selenium.upload.success",
        "ticket.created",
        "ticket.list.retrieved",
        "notification.schedule",
        "notification.schedule.document",
        "ticket.ticket.created",
        #"selenium.upload.success",
        #"wishlist.wishlist.created"
    )
    await consumer.start()
    scheduler_task = start_scheduler()
    yield
    await kafka_producer.stop()
    await consumer.stop()
    scheduler_task.cancel()

app = FastAPI(title="Notification Service", lifespan=lifespan)
app.include_router(router, prefix="/api/v1")