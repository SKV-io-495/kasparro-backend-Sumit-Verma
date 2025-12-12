import pytest
import datetime
import traceback
from sqlalchemy.future import select
from app.db.models import CryptoMarketData, EtlCheckpoint
from app.ingestion.pipeline import process_source
from app.schemas.crypto import CryptoUnifiedData
from app.core import database

# Define mock locally to avoid any import confusion
async def mock_fetch_test():
    return [
        CryptoUnifiedData(
            ticker="TEST", 
            price_usd=1.0, 
            source="test_source_iso",
            timestamp=datetime.datetime.now(datetime.timezone.utc)
        )
    ]

@pytest.mark.asyncio
async def test_etl_pipeline_success():
    source_name = "test_source_iso"
    
    # 1. Run Pipeline
    try:
        data = await mock_fetch_test()
        await process_source(source_name, mock_fetch_test)
    except Exception as e:
        traceback.print_exc()
        pytest.fail(f"Pipeline failed: {e}")

    # 2. Check Database
    async with database.AsyncSessionLocal() as session:
        # Checkpoint
        result = await session.execute(select(EtlCheckpoint).where(EtlCheckpoint.source_name == source_name))
        cp = result.scalars().first()
        
        assert cp is not None
        assert cp.records_processed == 1
        
        # CryptoMarketData
        result_data = await session.execute(select(CryptoMarketData).where(CryptoMarketData.source == source_name))
        rows = result_data.scalars().all()
        assert len(rows) == 1
