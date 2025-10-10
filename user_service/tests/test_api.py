from httpx import AsyncClient
import pytest

pytestmark = pytest.mark.asyncio


async def test_read_root(client: AsyncClient):
    response = await client.get("/api/v1/")
    assert response.status_code == 200
    assert response.json() == {"service": "User Service", "status": "ok"}


async def test_create_user(client: AsyncClient):
    user_data = {"telegram_id": 12345, "username": "testuser"}
    response = await client.post("/api/v1/users/", json=user_data)

    assert response.status_code == 200
    response_data = response.json()
    assert response_data["telegram_id"] == user_data["telegram_id"]
    assert "user_id" in response_data


async def test_create_duplicate_user(client: AsyncClient):
    user_data = {"telegram_id": 54321, "username": "duplicate_user"}

    response1 = await client.post("/api/v1/users/", json=user_data)
    assert response1.status_code == 200

    response2 = await client.post("/api/v1/users/", json=user_data)
    assert response2.status_code == 409
    assert "already exists" in response2.json()["detail"]