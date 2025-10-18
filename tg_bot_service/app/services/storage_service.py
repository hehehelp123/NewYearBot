import logging
import io
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
        logger.info("MinIO client initialized in Bot Service.")

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