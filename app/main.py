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

@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = (time.time() - start_time) * 1000
    response.headers["X-Process-Time"] = str(process_time)
    logger.info("http_request", method=request.method, path=request.url.path, duration_ms=process_time, status=response.status_code)
    return response

@app.get("/health")
async def health_check(db: AsyncSession = Depends(get_db)):
    start_time = time.time()
    db_status = "unhealthy"
    try:
        await db.execute(select(1))
        db_status = "connected"
    except Exception as e:
        db_status = f"error: {str(e)}"
    
    latency = (time.time() - start_time) * 1000
    
    return {
        "status": "ok",
        "phase": 2,
        "db_connectivity": db_status,
        "latency_ms": round(latency, 2)
    }

app.include_router(api_router)
