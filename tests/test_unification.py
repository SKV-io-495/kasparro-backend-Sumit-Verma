import pytest
import datetime
from sqlalchemy.future import select
from sqlalchemy import func
from app.db.models import CryptoMarketData
from app.ingestion.pipeline import process_source
from app.schemas.crypto import CryptoUnifiedData
from app.core import database

# Timestamp for consistent testing
TEST_TIMESTAMP = datetime.datetime.now(datetime.timezone.utc)

async def mock_fetch_source_a():
    return [
        CryptoUnifiedData(
            ticker="BTC", 
            price_usd=100.0, 
            source="source_a",
            timestamp=TEST_TIMESTAMP
        )
    ]

async def mock_fetch_source_b():
    return [
        CryptoUnifiedData(
            ticker="BTC", 
            price_usd=200.0, 
            source="source_b",
            timestamp=TEST_TIMESTAMP
        )
    ]

@pytest.mark.asyncio
async def test_unification_logic():
    # Clear DB for this test (or rely on transactional rollback if pytest uses it, 
    # but here we are in an async session, let's just count rows for specific ticker)
    
    # 1. Ingest Source A
    await process_source("source_a", mock_fetch_source_a)
    
    async with database.AsyncSessionLocal() as session:
        result = await session.execute(select(CryptoMarketData).where(CryptoMarketData.ticker == "BTC"))
        rows = result.scalars().all()
        assert len(rows) == 1
        record = rows[0]
        assert record.price_usd == 100.0
        assert record.sources_metadata["source_a"]["price"] == 100.0
        assert "source_b" not in record.sources_metadata

    # 2. Ingest Source B (Same timestamp)
    await process_source("source_b", mock_fetch_source_b)
    
    async with database.AsyncSessionLocal() as session:
        # Should still be 1 row, but updated
        result = await session.execute(select(CryptoMarketData).where(CryptoMarketData.ticker == "BTC"))
        rows = result.scalars().all()
        assert len(rows) == 1
        record = rows[0]
        
        # Check Unification
        assert record.sources_metadata["source_a"]["price"] == 100.0
        assert record.sources_metadata["source_b"]["price"] == 200.0
        
        # Check Averaging (100 + 200) / 2 = 150
        assert record.price_usd == 150.0
