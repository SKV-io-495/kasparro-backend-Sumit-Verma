import pytest
import datetime
from sqlalchemy.future import select
from app.db.models import Base, EtlCheckpoint
from app.ingestion.pipeline import process_source
from app.schemas.data import UnifiedDataCreate
from app.core import database
import traceback

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
    return [
        {"id": "test-1", "val": "100", "ts": datetime.datetime.now().isoformat()}
    ]

async def mock_fetch_fail():
    raise Exception("Simulated API Connection Error")

def mock_normalize(record):
    return UnifiedDataCreate(
        external_id=record["id"],
        name="Test",
        timestamp=datetime.datetime.fromisoformat(record["ts"]),
        value=record["val"],
        category="test"
    )

@pytest.mark.asyncio
async def test_etl_success_and_checkpoint():
    print("DEBUG: Starting success test")
    
    try:
        await process_source("test_source_success", mock_fetch_success, mock_normalize)
    except Exception as e:
        traceback.print_exc()
        pytest.fail(f"Process source failed: {e}")
    
    print("DEBUG: Finished process_source")

    async with database.AsyncSessionLocal() as session:
        print("DEBUG: Querying checkpoint")
        result = await session.execute(select(EtlCheckpoint).where(EtlCheckpoint.source_name == "test_source_success"))
        cp = result.scalars().first()
        
        if cp is None:
            # Debug: List all checkpoints
            all_res = await session.execute(select(EtlCheckpoint))
            all_cps = all_res.scalars().all()
            print(f"DEBUG: All checkpoints: {[c.source_name for c in all_cps]}")

        assert cp is not None, "Checkpoint not found"
        assert cp.last_status == "success"
        assert cp.records_processed == 1

@pytest.mark.asyncio
async def test_etl_failure_handling():
    await process_source("test_source_fail", mock_fetch_fail, mock_normalize)
    
    async with database.AsyncSessionLocal() as session:
        result = await session.execute(select(EtlCheckpoint).where(EtlCheckpoint.source_name == "test_source_fail"))
        cp = result.scalars().first()
        assert cp is not None
        assert cp.last_status == "failure"
        assert "Connection Error" in cp.error_log
