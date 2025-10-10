from httpx import AsyncClient
import pytest

pytestmark = pytest.mark.asyncio


async def test_read_root(client: AsyncClient):
    response = await client.get("/api/v1/")
    assert response.status_code == 200
    assert response.json() == {"service": "Ticket Service", "status": "ok"}


async def test_create_ticket(client: AsyncClient):
    ticket_data = {"requester_user_id": 1, "title": "Help with my account"}
    response = await client.post("/api/v1/tickets/", json=ticket_data)

    assert response.status_code == 200
    response_data = response.json()
    assert response_data["title"] == ticket_data["title"]
    assert "ticket_id" in response_data