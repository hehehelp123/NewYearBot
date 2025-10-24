import logging
import random
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

    def list_folders(self, prefix: str) -> list[str]:
        try:
            objects = self.client.list_objects(settings.MINIO_BUCKET, prefix=prefix, recursive=False)
            folders = []
            for obj in objects:
                if obj.is_dir:
                    folder_name = obj.object_name.strip(prefix).strip('/')
                    if folder_name:
                        folders.append(folder_name)
            return sorted(folders, reverse=True)
        except Exception as e:
            logger.error(f"Failed to list folders with prefix {prefix}: {e}", exc_info=True)
            return []

    def list_media(self, folder: str) -> list[dict]:
        media_list = []
        try:
            objects = self.client.list_objects(settings.MINIO_BUCKET, prefix=folder, recursive=True)
            for obj in objects:
                if not obj.is_dir and (obj.object_name.endswith(('.jpg', '.jpeg', '.png', '.mp4', '.mov', '.avi'))):
                    media_type = "video" if obj.object_name.lower().endswith(('.mp4', '.mov', '.avi')) else "photo"

                    presigned_url = self.client.presigned_get_object(
                        settings.MINIO_BUCKET,
                        obj.object_name,
                        expires=datetime.timedelta(hours=1)
                    )

                    media_list.append({
                        "object_name": obj.object_name,
                        "type": media_type,
                        "url": presigned_url
                    })
            return media_list
        except Exception as e:
            logger.error(f"Failed to list media in folder {folder}: {e}", exc_info=True)
            return []


storage_service = StorageService()