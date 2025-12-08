import pytest
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from app.core import database
from app.db.models import Base

# 1. Session-Scoped Event Loop
@pytest.fixture(scope="session")
def event_loop():
    """
    Creates a single event loop for the entire test session.
    """
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

# 2. Session-Scoped Engine
@pytest.fixture(scope="session")
async def db_engine():
    """
    Creates the SQLAlchemy AsyncEngine once per session.
    """
    engine = create_async_engine(database.settings.DATABASE_URL, echo=True)
    yield engine
    await engine.dispose()

# 3. Database Initialization (Create Tables)
@pytest.fixture(scope="session", autouse=True)
async def init_db(db_engine):
    """
    Creates tables before tests, drops them after.
    """
    async with db_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    
    yield
    
    async with db_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

# 4. Function-Scoped Session Patch
@pytest.fixture(scope="function", autouse=True)
async def patch_db_manager(db_engine):
    """
    Overrides the global db_manager to use the test session-scoped engine.
    Also ensures clean data state (rollback/truncate) could be handled here if needed.
    """
    original_engine = database.db_manager._engine
    original_maker = database.db_manager._session_maker
    
    database.db_manager._engine = db_engine
    database.db_manager._session_maker = sessionmaker(
        bind=db_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )
    
    yield
    
    # Restore global state
    database.db_manager._engine = original_engine
    database.db_manager._session_maker = original_maker
