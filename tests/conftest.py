import pytest
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from app.core import database
# Explicitly import all models so they are registered with Base.metadata
from app.db.models import Base, EtlCheckpoint, UnifiedData, RawData

# 1. Session-Scoped Event Loop
# Ensures a single loop for the whole test session (avoids "Task attached to different loop")
@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

# 2. Session-Scoped Engine
# Attached to the session loop
@pytest.fixture(scope="session")
async def db_engine():
    engine = create_async_engine(database.settings.DATABASE_URL, echo=True)
    yield engine
    await engine.dispose()

# 3. Function-Scoped DB Setup & Patch
# We use one fixture to handle both patching and schema creation for each test.
# This ensures strict ordering: Patch -> Create Tables -> Run Test -> Drop Tables -> Unpatch
@pytest.fixture(scope="function", autouse=True)
async def setup_test_db(db_engine):
    # --- Snapshot Global State ---
    original_engine = database.db_manager._engine
    original_maker = database.db_manager._session_maker
    
    # --- Patch Global Manager ---
    database.db_manager._engine = db_engine
    database.db_manager._session_maker = sessionmaker(
        bind=db_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )
    
    # --- Truncate Tables (Clean State) ---
    # We assume schema is created by app.init_db_script (CI) or locally.
    # We just clean data between tests.
    async with db_engine.begin() as conn:
        for table in reversed(Base.metadata.sorted_tables):
            await conn.execute(table.delete())
    
    yield
    
    # --- Cleanup (Optional: Truncate again) ---
    async with db_engine.begin() as conn:
        for table in reversed(Base.metadata.sorted_tables):
            await conn.execute(table.delete())
        
    # --- Restore Global State ---
    database.db_manager._engine = original_engine
    database.db_manager._session_maker = original_maker
