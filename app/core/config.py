from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache

class Settings(BaseSettings):
    PROJECT_NAME: str = "kasparro-etl"
    DATABASE_URL: str
    
    # Feature Flags
    CHAOS_MODE: bool = False

    model_config = SettingsConfigDict(
        case_sensitive=True,
        env_file=".env",
        extra="ignore"
    )

@lru_cache()
def get_settings():
    return Settings()
