import pytest
import asyncio
from typing import Generator, AsyncGenerator
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from app.core import database
from app.db.models import Base

# Force session scope for event loop if needed, OR function scope. 
# Common fix for "attached to different loop" is to ensure engine is created inside the loop context.
@pytest.fixture(scope="function")
def event_loop():
    """
    Creates a fresh event loop for each test function.
    """
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(scope="function", autouse=True)
async def setup_test_db():
    """
    Overrides the global db_manager with a fresh engine/schema for each test.
    """
    # 1. Create a fresh engine for this test's loop
    engine = create_async_engine(database.settings.DATABASE_URL, echo=False)
    
    # 2. Monkeypatch the global manager
    database.db_manager._engine = engine
    database.db_manager._session_maker = sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )

    # 3. Create Schema (Fresh DB)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    yield

    # 4. Cleanup
    await engine.dispose()
    database.db_manager._engine = None
    database.db_manager._session_maker = None
