from httpx import AsyncClient
import pytest

pytestmark = pytest.mark.asyncio


async def test_read_root(client: AsyncClient):
    response = await client.get("/api/v1/")
    assert response.status_code == 200
    assert response.json() == {"service": "Wishlist Service", "status": "ok"}


async def test_create_wishlist(client: AsyncClient):
    wishlist_data = {"owner_user_id": 1, "name": "My Birthday"}
    response = await client.post("/api/v1/wishlists/", json=wishlist_data)

    assert response.status_code == 200
    response_data = response.json()
    assert response_data["name"] == wishlist_data["name"]
    assert "wishlist_id" in response_data