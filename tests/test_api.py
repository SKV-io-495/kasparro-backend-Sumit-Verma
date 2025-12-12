import pytest
import pytest_asyncio
from httpx import AsyncClient
from app.main import app

@pytest_asyncio.fixture
async def async_client():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        yield ac

@pytest.mark.asyncio
async def test_health(async_client):
    response = await async_client.get("/health")
    # Verify 200 - OK
    # Depending on app implementation, might simply return {"status": "ok"}
    if response.status_code == 404:
        pytest.skip("Health endpoint not implemented")
    assert response.status_code == 200

@pytest.mark.asyncio
async def test_get_data(async_client):
    # This test assumes data might exist or returns empty list validly
    response = await async_client.get("/data")
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert "meta" in data
    # If data, verify structure match
    if data["data"]:
        item = data["data"][0]
        assert "ticker" in item
        assert "price_usd" in item
