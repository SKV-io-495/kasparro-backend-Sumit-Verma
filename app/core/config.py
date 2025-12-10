from pydantic_settings import BaseSettings
from functools import lru_cache

class Settings(BaseSettings):
    PROJECT_NAME: str = "kasparro-etl"
    DATABASE_URL: str
    
    # Feature Flags
    CHAOS_MODE: bool = False

    class Config:
        case_sensitive = True
        env_file = ".env"

@lru_cache()
def get_settings():
    return Settings()
