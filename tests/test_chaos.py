import pytest
import datetime
import asyncio
from sqlalchemy.future import select
from app.db.models import Base, EtlCheckpoint
from app.ingestion.pipeline import process_source
from app.schemas.data import UnifiedDataCreate
from app.core import database, config

# --- Setup ---
@pytest.fixture(autouse=True)
async def setup_db():
    database.db_manager._engine = None 
    database.db_manager._session_maker = None
    
    engine = database.db_manager.engine
    
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield
    await engine.dispose()

# --- Mocks ---
async def mock_fetch_success():
    # Return enough records to trigger chaos (> 50%)
    # If 5 records, fail after 3.
    return [
        {"id": f"chaos-{i}", "val": "100", "ts": datetime.datetime.now().isoformat()}
        for i in range(5)
    ]

def mock_normalize(record):
    return UnifiedDataCreate(
        external_id=record["id"],
        name="Chaos Test",
        timestamp=datetime.datetime.fromisoformat(record["ts"]),
        value=record["val"],
        category="test"
    )

@pytest.mark.asyncio
async def test_chaos_recovery(monkeypatch):
    """
    1. Enable Chaos Mode.
    2. Run ETL -> Should fail mid-stream.
    3. Verify Checkpoint is FAILURE.
    4. Disable Chaos Mode.
    5. Run ETL -> Should resume and complete.
    6. Verify Checkpoint is SUCCESS.
    """
    settings = config.get_settings()
    monkeypatch.setattr(settings, "CHAOS_MODE", True)
    
    # 1. First Run (Chaotic)
    print("DEBUG: Running Chaos Iteration")
    await process_source("chaos_source", mock_fetch_success, mock_normalize)
    
    async with database.AsyncSessionLocal() as session:
        result = await session.execute(select(EtlCheckpoint).where(EtlCheckpoint.source_name == "chaos_source"))
        cp = result.scalars().first()
        assert cp is not None
        assert cp.last_status == "failure"
        assert "CHAOS_MODE_TRIGGERED" in cp.error_log
    
    # 2. Resumption (Order)
    print("DEBUG: Running Resumption Iteration")
    monkeypatch.setattr(settings, "CHAOS_MODE", False)
    
    await process_source("chaos_source", mock_fetch_success, mock_normalize)
    
    async with database.AsyncSessionLocal() as session:
        result = await session.execute(select(EtlCheckpoint).where(EtlCheckpoint.source_name == "chaos_source"))
        cp = result.scalars().first()
        assert cp is not None
        assert cp.last_status == "success"
        assert cp.records_processed > 0
        # In a real resume logic, we would skip duplicates.
        # Our pipeline logic checks idempotency via ON CONFLICT.
