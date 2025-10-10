import httpx

class HttpClient:
    def __init__(self):
        self.client = httpx.AsyncClient()

    async def start(self):
        pass

    async def stop(self):
        await self.client.aclose()

http_client = HttpClient()