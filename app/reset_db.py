import asyncio
from app.core.database import db_manager
from app.db.models import Base

async def reset_db():
    print("Resetting database...")
    engine = db_manager.engine
    async with engine.begin() as conn:
        print("Dropping all tables...")
        await conn.run_sync(Base.metadata.drop_all)
        print("Creating all tables...")
        await conn.run_sync(Base.metadata.create_all)
    print("Database reset complete.")
    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(reset_db())
