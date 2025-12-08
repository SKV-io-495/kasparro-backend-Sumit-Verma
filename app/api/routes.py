import time
import uuid
from typing import List, Optional

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func

from app.core.database import get_db
from app.db.models import UnifiedData, EtlCheckpoint
from app.schemas.data import UnifiedDataResponse, PaginatedResponse, MetaData

router = APIRouter()

@router.get("/data", response_model=PaginatedResponse[UnifiedDataResponse])
async def get_data(
    request: Request,
    offset: int = 0,
    limit: int = 10,
    category: Optional[str] = Query(None, description="Filter by category"),
    db: AsyncSession = Depends(get_db)
):
    start_time = time.time()
    query = select(UnifiedData)
    
    if category:
        query = query.where(UnifiedData.category == category)
    
    query = query.offset(offset).limit(limit)
    
    result = await db.execute(query)
    data = result.scalars().all()
    
    latency = (time.time() - start_time) * 1000
    request_id = str(uuid.uuid4())
    
    return PaginatedResponse(
        meta=MetaData(request_id=request_id, latency_ms=latency),
        data=data
    )

@router.get("/stats")
async def get_stats(db: AsyncSession = Depends(get_db)):
    """
    Returns metrics about the ETL pipeline.
    """
    result = await db.execute(select(EtlCheckpoint))
    checkpoints = result.scalars().all()
    
    stats = []
    for cp in checkpoints:
        stats.append({
            "source_name": cp.source_name,
            "status": cp.last_status,
            "records_processed": cp.records_processed,
            "last_ingested_at": cp.last_ingested_timestamp,
            "duration_ms": cp.run_duration_ms,
            "error_log": cp.error_log
        })
        
    return {"etl_stats": stats}

from app.services.etl_service import trigger_etl_job

@router.post("/etl/run")
async def run_etl_job():
    """
    Manually triggers the ETL pipeline.
    """
    try:
        await trigger_etl_job()
        return {"status": "triggered", "message": "ETL job started successfully"}
    except Exception as e:
        return {"status": "failed", "error": str(e)}
