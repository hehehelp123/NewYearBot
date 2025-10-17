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
        logger.info("MinIO client initialized.")

    def upload_file(self, object_name: str, file_path: str, content_type: str):
        try:
            self.client.fput_object(
                settings.MINIO_BUCKET,
                object_name,
                file_path,
                content_type=content_type,
            )
            logger.info(f"File {object_name} uploaded successfully to bucket {settings.MINIO_BUCKET}.")
            return True
        except Exception as e:
            logger.error(f"Failed to upload file {object_name}: {e}", exc_info=True)
            return False

    def download_file(self, object_name: str, file_path: str):
        try:
            self.client.fget_object(settings.MINIO_BUCKET, object_name, file_path)
            logger.info(f"File {object_name} downloaded successfully to {file_path}.")
            return True
        except Exception as e:
            logger.error(f"Failed to download file {object_name}: {e}", exc_info=True)
            return False

    def delete_file(self, object_name: str):
        try:
            self.client.remove_object(settings.MINIO_BUCKET, object_name)
            logger.info(f"File {object_name} deleted successfully from bucket {settings.MINIO_BUCKET}.")
            return True
        except Exception as e:
            logger.error(f"Failed to delete file {object_name}: {e}", exc_info=True)
            return False

storage_service = StorageService()