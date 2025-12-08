from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from app.core.config import get_settings

settings = get_settings()

# Lazy initialization to prevent import-time loop binding issues
class Database:
    def __init__(self):
        self._engine = None
        self._session_maker = None

    @property
    def engine(self):
        if self._engine is None:
            self._engine = create_async_engine(settings.DATABASE_URL, echo=True)
        return self._engine

    @property
    def session_maker(self):
        if self._session_maker is None:
            self._session_maker = sessionmaker(
                bind=self.engine,
                class_=AsyncSession,
                expire_on_commit=False,
                autoflush=False,
            )
        return self._session_maker

db_manager = Database()

async def get_db():
    async with db_manager.session_maker() as session:
        yield session

# Helper for non-dependency contexts (like ETL)
def AsyncSessionLocal():
    return db_manager.session_maker()
