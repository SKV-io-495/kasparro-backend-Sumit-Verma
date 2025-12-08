import asyncio
import csv
import time
# feedparser imported locally to avoid test collection errors
import traceback
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.future import select
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from prometheus_client import Counter, Histogram, Gauge

from app.core.database import AsyncSessionLocal
from app.db.models import RawData, UnifiedData, EtlCheckpoint
from app.schemas.data import UnifiedDataCreate
from app.core import config
from app.core.logging_config import get_logger
from app.services.drift_detection import detect_drift

logger = get_logger("etl_pipeline")
settings = config.get_settings()

# --- Metrics ---
ETL_RECORDS_PROCESSED = Counter('etl_records_processed_total', 'Total records processed', ['source'])
ETL_RUN_DURATION = Histogram('etl_run_duration_seconds', 'ETL run duration', ['source'])
ETL_JOB_STATUS = Gauge('etl_job_status', 'ETL job status (1=Success, 0=Fail)', ['source'])

# --- Checkpoint Logic ---

async def get_checkpoint(session, source_name: str) -> Optional[EtlCheckpoint]:
    result = await session.execute(
        select(EtlCheckpoint).where(EtlCheckpoint.source_name == source_name)
    )
    return result.scalars().first()

async def update_checkpoint(session, source_name: str, status: str, records: int, duration: int, error: Optional[str] = None, last_ts: Optional[datetime] = None):
    # Update Metrics
    ETL_JOB_STATUS.labels(source=source_name).set(1 if status == "success" else 0)
    
    # Simple Select + Update/Insert
    result = await session.execute(select(EtlCheckpoint).where(EtlCheckpoint.source_name == source_name))
    cp = result.scalars().first()
    
    if cp:
        cp.last_status = status
        cp.records_processed = records
        cp.run_duration_ms = duration
        cp.error_log = error
        cp.last_ingested_timestamp = last_ts
    else:
        cp = EtlCheckpoint(
            source_name=source_name,
            last_status=status,
            records_processed=records,
            run_duration_ms=duration,
            error_log=error,
            last_ingested_timestamp=last_ts
        )
        session.add(cp)
    
    # We rely on caller to commit

# --- Extraction Sources ---

# Using exponential backoff to handle transient network spikes
@retry(
    stop=stop_after_attempt(5), 
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry_error_callback=lambda retry_state: logger.error("retry_failed", source="mock_api", attempts=retry_state.attempt_number)
)
async def fetch_mock_api_data() -> List[Dict[str, Any]]:
    # Simulate partial failure for Retry testing?
    # For now, standard mock.
    await asyncio.sleep(0.5)
    return [
        {
            "id": "api-101",
            "device_name": "Sensor-X",
            "read_at": datetime.now(timezone.utc).isoformat(),
            "reading": "55.5",
            "type": "temp"
        },
        {
            "id": "api-102",
            "device_name": "Sensor-Y",
            "read_at": datetime.now(timezone.utc).isoformat(),
            "reading": "60.2",
            "type": "temp"
        }
    ]

def read_csv_data(file_path: str = "data/source.csv") -> List[Dict[str, Any]]:
    results = []
    try:
        with open(file_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                results.append(row)
    except FileNotFoundError:
        logger.warning("csv_not_found", path=file_path)
    return results

@retry(
    stop=stop_after_attempt(5), 
    wait=wait_exponential(multiplier=1, min=4, max=10)
)
async def fetch_rss_feed(url: str = "http://feeds.bbci.co.uk/news/rss.xml") -> List[Dict[str, Any]]:
    import feedparser
    # In real world, we would use aiohttp + feedparser
    # Just returning mock to avoid network deps in container for now
    return [
        {
            "link": "rss-001",
            "title": "News Item 1",
            "published": datetime.now(timezone.utc).isoformat(),
            "summary": "Some news content",
            "category": "news"
        }
    ]

# --- Normalization ---

def normalize_api_record(record: Dict[str, Any]) -> UnifiedDataCreate:
    # Drift Check?
    # Ideally checking against model is hard unless we map dict to source model first.
    # We will do drift check in process_source using raw record keys.
    return UnifiedDataCreate(
        external_id=record["id"],
        name=record["device_name"],
        timestamp=datetime.fromisoformat(record["read_at"]),
        value=record["reading"],
        category=record["type"]
    )

def normalize_csv_record(record: Dict[str, Any]) -> UnifiedDataCreate:
    return UnifiedDataCreate(
        external_id=record["external_id"],
        name=record["name"],
        timestamp=datetime.now(timezone.utc), 
        value=record["val"],
        category=record["cat"]
    )

def normalize_rss_record(record: Dict[str, Any]) -> UnifiedDataCreate:
    ts = datetime.fromisoformat(record["published"]) if "published" in record else datetime.now(timezone.utc)
    return UnifiedDataCreate(
        external_id=record["link"],
        name=record["title"],
        timestamp=ts,
        value=record["summary"], # Mapping summary to value
        category=record.get("category", "rss")
    )

# --- Loading ---

async def process_source(source_name: str, fetch_func, normalize_func):
    start_time = time.time()
    logger.info("etl_start", source=source_name)
    
    async with AsyncSessionLocal() as session:
        # 1. Checkpoint
        checkpoint = await get_checkpoint(session, source_name)
        last_ingested = checkpoint.last_ingested_timestamp if checkpoint and checkpoint.last_status == "success" else None
        
        try:
            # 2. Extract
            raw_records = await asyncio.to_thread(fetch_func) if not asyncio.iscoroutinefunction(fetch_func) else await fetch_func()
            logger.info("fetched_records", source=source_name, count=len(raw_records))

            # 3. Transform & Filter
            new_records = []
            max_ts = last_ingested
            
            # Chaos Mode Counter
            processed_count = 0
            total_records = len(raw_records)

            for rec in raw_records:
                try:
                    # Drift Detection (on first record or sample)
                    if processed_count == 0:
                        # We guess the model based on normalize_func or pass it explicitly?
                        # For now, skipping explicit drift model mapping for simplicity or inferring.
                        # Real drift detection needs expected schema. 
                        # We'll just log keys for now as a basic drift check.
                        detect_drift(rec, UnifiedDataCreate, source_name) # Using Target model as proxy? No, source schema needed.
                        # Ideally we pass SourceModel to process_source. 

                    unified = normalize_func(rec)
                    
                    if last_ingested and unified.timestamp and unified.timestamp <= last_ingested:
                        continue
                        
                    new_records.append(unified)
                    processed_count += 1

                    # Chaos Injection
                    if settings.CHAOS_MODE and processed_count > (total_records / 2):
                        raise Exception("CHAOS_MODE_TRIGGERED: Simulated failure mid-stream")
                    
                    if unified.timestamp:
                        if max_ts is None or unified.timestamp > max_ts:
                            max_ts = unified.timestamp
                            
                except Exception as e:
                    if "CHAOS" in str(e): raise e # Re-raise chaos
                    logger.warning("validation_error", source=source_name, error=str(e))
                    continue

            # 4. Load
            if new_records:
                for unified_data in new_records:
                    stmt = insert(UnifiedData).values(
                        external_id=unified_data.external_id,
                        name=unified_data.name,
                        timestamp=unified_data.timestamp,
                        value=unified_data.value,
                        category=unified_data.category
                    ).on_conflict_do_update(
                        index_elements=['external_id'],
                        set_={
                            "name": unified_data.name,
                            "timestamp": unified_data.timestamp,
                            "value": unified_data.value,
                            "category": unified_data.category
                        }
                    )
                    await session.execute(stmt)
            
            await session.commit()
            
            # 5. Update Checkpoint Success
            duration_ms = int((time.time() - start_time) * 1000)
            duration_sec = duration_ms / 1000.0
            
            await update_checkpoint(session, source_name, "success", len(new_records), duration_ms, last_ts=max_ts)
            await session.commit()
            
            logger.info("etl_success", source=source_name, records=len(new_records), duration_ms=duration_ms)
            
            # Metrics Update
            ETL_RECORDS_PROCESSED.labels(source=source_name).inc(len(new_records))
            ETL_RUN_DURATION.labels(source=source_name).observe(duration_sec)

        except Exception as e:
            await session.rollback()
            logger.error("etl_failure", source=source_name, error=str(e))
            traceback.print_exc()
            duration_ms = int((time.time() - start_time) * 1000)
            await update_checkpoint(session, source_name, "failure", 0, duration_ms, error=str(e))
            await session.commit()

async def run_etl():
    logger.info("pipeline_start", phase=3)
    try:
        await process_source("mock_api", fetch_mock_api_data, normalize_api_record)
        await process_source("local_csv", read_csv_data, normalize_csv_record)
        await process_source("rss_feed", fetch_rss_feed, normalize_rss_record)
    except Exception as e:
         logger.critical("pipeline_critical_failure", error=str(e))
    logger.info("pipeline_finish")

if __name__ == "__main__":
    asyncio.run(run_etl())
