from app.core.database import db_manager
from app.db.models import Base

async def init_db():
    async with db_manager.engine.begin() as conn:
        # await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
