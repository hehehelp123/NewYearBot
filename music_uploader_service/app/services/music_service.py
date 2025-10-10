import logging
from app.kafka.producer import kafka_producer
from app.schemas.music_schemas import MusicUpload

logger = logging.getLogger(__name__)


class MusicService:
    async def process_upload(self, upload_data: MusicUpload):
        logger.info(f"Processing upload for user {upload_data.user_id}: {upload_data.file_name}")

        event_data = {
            "user_id": upload_data.user_id,
            "status": "success",
            "title": upload_data.title,
            "artist": upload_data.artist
        }
        await kafka_producer.send("music_upload_status", event_data)

        return {"status": "processing", "details": upload_data.model_dump()}


music_service = MusicService()