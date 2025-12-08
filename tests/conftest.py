import pytest
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from app.core import database
from app.db.models import Base

# 1. Session-Scoped Event Loop
# This overrides the default pytest-asyncio loop management to use a single loop for the whole session.
# Crucial for resolving "Task attached to a different loop" errors.
@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

# 2. Session-Scoped Engine
# Created ONCE per session, attached to the session loop.
@pytest.fixture(scope="session")
async def db_engine():
    engine = create_async_engine(database.settings.DATABASE_URL, echo=False)
    
    # Initialize Schema
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    yield engine
    
    await engine.dispose()

# 3. Function-Scoped Patch
# Applies the session engine to the global db_manager for every test.
# Cleans up data between tests.
@pytest.fixture(scope="function", autouse=True)
async def patch_db_manager(db_engine):
    # Snapshot original state
    original_engine = database.db_manager._engine
    original_maker = database.db_manager._session_maker
    
    # Patch Global Manager
    database.db_manager._engine = db_engine
    database.db_manager._session_maker = sessionmaker(
        bind=db_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )
    
    yield
    
    # Cleanup Data (Truncate/Recreate)
    async with db_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    
    # Restore State
    database.db_manager._engine = original_engine
    database.db_manager._session_maker = original_maker
