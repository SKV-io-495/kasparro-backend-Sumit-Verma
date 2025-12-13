import time
from fastapi import FastAPI, Depends, Request
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.db.init_db import init_db
from app.services.etl_service import trigger_etl_job
from app.api.routes import router as api_router

from prometheus_fastapi_instrumentator import Instrumentator
from app.core.logging_config import setup_logging, get_logger

# Setup Structured Logging
setup_logging()
logger = get_logger("main")

app = FastAPI(title="kasparro-etl")

# Instrument Prometheus
Instrumentator().instrument(app).expose(app)

@app.on_event("startup")
async def startup_event():
    try:
        await init_db()
    except Exception as e:
        logger.error("db_init_failed", error=str(e))
    logger.info("startup_event", msg="Initializing DB and running ETL")
    try:
        await trigger_etl_job()
    except Exception as e:
        logger.error("etl_startup_failed", error=str(e))

# @app.middleware("http")
# async def add_process_time_header(request: Request, call_next):
#     start_time = time.time()
#     response = await call_next(request)
#     process_time = (time.time() - start_time) * 1000
#     response.headers["X-Process-Time"] = str(process_time)
#     logger.info("http_request", method=request.method, path=request.url.path, duration_ms=process_time, status=response.status_code)
#     return response

@app.get("/health")
async def health_check(db: AsyncSession = Depends(get_db)):
    start_time = time.time()
    db_status = "unhealthy"
    etl_status = "unknown"
    last_run = None
    
    try:
        # Check DB connectivity
        await db.execute(select(1))
        db_status = "connected"
        
        # Check ETL Status (P0.2 Requirement)
        # Fetch the most recent checkpoint update
        # We order by - id or timestamp? Timestamp might be null if never succeeded depending on logic, 
        # but 'last_ingested_timestamp' is updated on success. 
        # 'id' is reliable for insertion order if we create new checkpoints, 
        # but the logic uses update_checkpoint which updates existing row per source.
        # So we want to check if ANY source failed recently or overall health.
        # Let's get all checkpoints and report summary or just the latest interaction.
        # The requirement says "return the latest ETL status". 
        # If we have multiple sources, and one failed, what is the status? 
        # Let's say we check if ANY source is in 'failure' state?
        # Or just pick the one with most recent 'last_ingested_timestamp'?
        
        # Let's just fetch all and decide.
        from app.db.models import EtlCheckpoint
        result = await db.execute(select(EtlCheckpoint))
        checkpoints = result.scalars().all()
        
        if not checkpoints:
            etl_status = "no_runs_yet"
        else:
            # If any reported failure, we might flag it, or simplistic approach:
            # Just take the status of the "latest" run.
            # But runs are concurrent.
            # Let's return "healthy" if all are success, else "degraded".
            # Or formatted list? Requirement says "etl_status field".
            failures = [cp for cp in checkpoints if cp.last_status != 'success']
            if failures:
                etl_status = "failure" # or degraded
            else:
                etl_status = "success"
                
            # For last_run, get the max timestamp
            timestamps = [cp.last_ingested_timestamp for cp in checkpoints if cp.last_ingested_timestamp]
            if timestamps:
                last_run = max(timestamps).isoformat()
                
    except Exception as e:
        db_status = f"error: {str(e)}"
    
    latency = (time.time() - start_time) * 1000
    
    return {
        "status": "ok",
        "phase": 2,
        "db_connectivity": db_status,
        "etl_status": etl_status,
        "last_run": last_run,
        "latency_ms": round(latency, 2)
    }

app.include_router(api_router)
