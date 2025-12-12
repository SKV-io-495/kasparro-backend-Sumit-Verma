"""
Handles API requests for market data and ETL statistics.
Serves as the gateway for the frontend dashboard to access normalized crypto metrics.
"""
import time
import uuid
from typing import List, Optional

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func

from app.core.database import get_db
from app.db.models import CryptoMarketData, EtlCheckpoint
from app.schemas.crypto import CryptoUnifiedDataResponse
from app.schemas.data import PaginatedResponse, MetaData

router = APIRouter()

@router.get("/data", response_model=PaginatedResponse[CryptoUnifiedDataResponse])
async def get_data(
    request: Request,
    offset: int = 0,
    limit: int = 10,
    ticker: Optional[str] = Query(None, description="Filter by ticker"),
    db: AsyncSession = Depends(get_db)
):
    start_time = time.time()
    query = select(CryptoMarketData)
    
    if ticker:
        query = query.where(CryptoMarketData.ticker == ticker.upper())
    
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

@router.get("/runs")
async def get_runs(limit: int = 10, db: AsyncSession = Depends(get_db)):
    """
    Get recent ETL runs (Checkpoints).
    """
    query = select(EtlCheckpoint).limit(limit)
    result = await db.execute(query)
    return result.scalars().all()

from app.ingestion.pipeline import run_etl

@router.post("/etl/run")
async def run_etl_job():
    """
    Manually triggers the ETL pipeline.
    """
    try:
        # Note: run_etl is async, we should await it if we want to wait for completion, 
        # or use background task if we want it async. 
        # The original code imported trigger_etl_job from services.etl_service which probably did it in bg?
        # But here I'll just await it for simplicity as per request to "Check Smoke Test"
        await run_etl()
        return {"status": "triggered", "message": "ETL job completed"}
    except Exception as e:
        return {"status": "failed", "error": str(e)}
