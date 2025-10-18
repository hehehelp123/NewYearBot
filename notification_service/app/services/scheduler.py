import asyncio
import logging
from app.core.db import AsyncSessionLocal
from app.services.notification_service import NotificationService

logger = logging.getLogger(__name__)

async def run_scheduler_check():
    logger.info("Running scheduled task to check for notifications...")
    try:
        async with AsyncSessionLocal() as session:
            service = NotificationService(session)
            await service.process_scheduled_notifications()
    except Exception as e:
        logger.error(f"Error during scheduled notification check: {e}", exc_info=True)

async def _scheduler_loop():
    while True:
        await run_scheduler_check()
        await asyncio.sleep(60)

def start_scheduler():
    logger.info("Starting scheduler background task.")
    task = asyncio.create_task(_scheduler_loop())
    return task