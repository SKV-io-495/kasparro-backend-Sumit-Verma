import pytest
import asyncio
from unittest.mock import AsyncMock, patch
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from app.core import database
# Explicit import to ensure metadata is populated
from app.db.models import Base, EtlCheckpoint, CryptoMarketData, RawData

# 1. Function-Scoped Event Loop
# Removed custom event_loop fixture; relying on pytest-asyncio 'auto' mode + pytest.ini configuration.
# This prevents conflict with recent pytest-asyncio versions.

# 2. Function-Scoped Engine
@pytest.fixture(scope="function")
async def db_engine():
    print("DEBUG: Creating DB Engine")
    engine = create_async_engine(database.settings.DATABASE_URL, echo=False)
    yield engine
    await engine.dispose()

# 3. Patch Startup Events (CRITICAL for AsyncClient)
@pytest.fixture(scope="function", autouse=True)
async def mock_startup_handlers():
    # Prevent real init_db and trigger_etl_job from running during tests
    # This avoids "Task attached to different loop" and DB conflicts
    with patch("app.main.init_db", new_callable=AsyncMock) as mock_init, \
         patch("app.main.trigger_etl_job", new_callable=AsyncMock) as mock_etl:
        yield mock_init, mock_etl

# 4. Function-Scoped DB Setup
@pytest.fixture(scope="function", autouse=True)
async def setup_test_db(db_engine):
    # Snapshot global
    original_engine = database.db_manager._engine
    original_maker = database.db_manager._session_maker
    
    # Patch global
    database.db_manager._engine = db_engine
    database.db_manager._session_maker = sessionmaker(
        bind=db_engine, class_=AsyncSession, expire_on_commit=False, autoflush=False
    )
    
    # NUCLEAR OPTION: DROP ALL then CREATE ALL
    print("DEBUG: NUCLEAR CLEANUP STARTing")
    async with db_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    print("DEBUG: NUCLEAR CLEANUP DONE")
    
    yield
    
    # Cleanup after test
    async with db_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        
    # Restore global
    database.db_manager._engine = original_engine
    database.db_manager._session_maker = original_maker
