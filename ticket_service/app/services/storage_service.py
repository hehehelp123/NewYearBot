import logging
import io
import uuid
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

    def upload_file_from_bytes(self, file_bytes: bytes, original_filename: str, folder: str = "default") -> str:
        """
        Загружает файл из байтов в MinIO и возвращает уникальный ключ объекта.
        Идеально для tg_bot_service.
        """
        try:
            object_name = f"{folder}/{uuid.uuid4()}-{original_filename}"

            self.client.put_object(
                settings.MINIO_BUCKET,
                object_name,
                io.BytesIO(file_bytes),
                len(file_bytes),
                content_type='application/octet-stream'
            )
            logger.info(f"File {original_filename} uploaded to MinIO as {object_name}")
            return object_name
        except Exception as e:
            logger.error(f"Failed to upload file from bytes: {e}", exc_info=True)
            raise

    def download_file_to_path(self, object_name: str, file_path: str):
        """
        Скачивает файл из MinIO в указанный путь на диске.
        Идеально для ticket_service.
        """
        try:
            self.client.fget_object(settings.MINIO_BUCKET, object_name, file_path)
            logger.info(f"File {object_name} downloaded successfully to {file_path}.")
        except Exception as e:
            logger.error(f"Failed to download file {object_name}: {e}", exc_info=True)
            raise

    def delete_file(self, object_name: str):
        """Удаляет файл из MinIO."""
        try:
            self.client.remove_object(settings.MINIO_BUCKET, object_name)
            logger.info(f"File {object_name} deleted successfully from bucket {settings.MINIO_BUCKET}.")
        except Exception as e:
            logger.error(f"Failed to delete file {object_name}: {e}", exc_info=True)
            raise


storage_service = StorageService()