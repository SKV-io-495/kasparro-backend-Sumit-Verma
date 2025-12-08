from pydantic import BaseModel
from typing import Optional, Any, List, Generic, TypeVar
from datetime import datetime

T = TypeVar('T')

class UnifiedDataCreate(BaseModel):
    external_id: str
    name: Optional[str] = None
    timestamp: Optional[datetime] = None
    value: Optional[str] = None
    category: Optional[str] = None

class UnifiedDataResponse(UnifiedDataCreate):
    id: int

    class Config:
        from_attributes = True

class RawDataCreate(BaseModel):
    source: str
    payload: Any

class MetaData(BaseModel):
    request_id: str
    latency_ms: float

class PaginatedResponse(BaseModel, Generic[T]):
    meta: MetaData
    data: List[T]
