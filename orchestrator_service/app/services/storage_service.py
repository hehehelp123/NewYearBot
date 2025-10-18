import logging
from minio import Minio
from app.core.config import settings

logger = logging.getLogger(__name__)

class StorageService:
    def __init__(self):
        self.client = Minio(
            settings.MINIO_URL,
            access_key=settings.MINIO_ROOT_USER,
            secret_key=settings.MINIO_ROOT_PASSWORD,
            secure=False
        )
        logger.info("MinIO client initialized in Orchestrator.")

    def upload_file_obj(self, object_name: str, file_data, file_len: int, content_type: str):
        try:
            self.client.put_object(
                settings.MINIO_BUCKET,
                object_name,
                file_data,
                length=file_len,
                content_type=content_type,
            )
            logger.info(f"File {object_name} uploaded successfully to bucket {settings.MINIO_BUCKET}.")
            return True
        except Exception as e:
            logger.error(f"Failed to upload file object {object_name}: {e}", exc_info=True)
            return False

storage_service = StorageService()