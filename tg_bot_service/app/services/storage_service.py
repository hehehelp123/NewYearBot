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

    def upload_file(self, file_bytes: bytes, original_filename: str) -> str:
        """Загружает файл в MinIO и возвращает уникальный ключ объекта."""
        try:
            object_name = f"tickets/{uuid.uuid4()}-{original_filename}"

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
            logger.error(f"Failed to upload file {original_filename}: {e}", exc_info=True)
            raise

    def download_file_as_bytes(self, object_name: str) -> bytes | None:
        try:
            response = self.client.get_object(settings.MINIO_BUCKET, object_name)
            file_bytes = response.read()
            return file_bytes
        except Exception as e:
            logger.error(f"Failed to download object {object_name} as bytes: {e}", exc_info=True)
            return None
        finally:
            if 'response' in locals() and response:
                response.close()
                response.release_conn()


storage_service = StorageService()