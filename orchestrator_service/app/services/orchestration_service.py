import httpx
from app.core.config import settings
from app.schemas.user_schemas import UserCreateRequest
from app.schemas.music_schemas import MusicSyncRequest

class OrchestrationService:
    def __init__(self):
        self.client = httpx.AsyncClient(timeout=60)

    async def register_user(self, user_data: UserCreateRequest):
        response = await self.client.post(
            f"{settings.USER_SERVICE_URL}/api/v1/users/",
            json=user_data.model_dump()
        )
        response.raise_for_status()
        return response.json()

    async def sync_yandex_music(self, payload: MusicSyncRequest):
        response = await self.client.post(
            f"{settings.MUSIC_UPLOADER_SERVICE_URL}/api/v1/sync/",
            json=payload.model_dump(mode="json")
        )
        response.raise_for_status()
        return response.json()

    async def close(self):
        await self.client.aclose()

orchestration_service = OrchestrationService()