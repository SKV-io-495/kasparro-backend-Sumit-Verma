import asyncio
import os
from sqlalchemy.ext.asyncio import create_async_engine
from app.db.models import Base, EtlCheckpoint, CryptoMarketData, RawData
from app.core import database

# Ensure DATABASE_URL is set (CI sets it, but fallback for safety)
DATABASE_URL = os.getenv("DATABASE_URL", database.settings.DATABASE_URL)

async def init_db():
    print(f"Initializing database at {DATABASE_URL}")
    engine = create_async_engine(DATABASE_URL, echo=True)
    
    async with engine.begin() as conn:
        print("Dropping existing tables...")
        await conn.run_sync(Base.metadata.drop_all)
        print("Creating tables...")
        await conn.run_sync(Base.metadata.create_all)
        print("Tables created successfully.")
    
    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(init_db())
