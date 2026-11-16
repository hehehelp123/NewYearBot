import httpx

class HttpClient:
    def __init__(self):
        # по умолчанию у httpx 5 с — видео из альбома через оркестратор столько не успевало скачаться
        self.client = httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=5.0))

    async def start(self):
        pass

    async def stop(self):
        await self.client.aclose()

http_client = HttpClient()