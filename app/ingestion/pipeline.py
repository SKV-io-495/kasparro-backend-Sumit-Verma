"""
Orchestrates async data fetching from CoinGecko/Paprika and handles normalization failures.
This pipeline is the central nervous system of the ETL process, ensuring data integrity and resilience.
"""
import asyncio
import time
import traceback
from datetime import datetime
from typing import List, Callable, Any

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.future import select

from app.core.database import AsyncSessionLocal
from app.db.models import CryptoMarketData, EtlCheckpoint, RawData
from app.schemas.crypto import CryptoUnifiedData
from app.core import config
from app.core.logging_config import get_logger
from app.services.drift_detection import detect_drift

# Import new sources
from app.ingestion.sources import coinpaprika, coingecko, csv_ingestor

logger = get_logger("etl_pipeline")
settings = config.get_settings()

# --- Metrics (Placeholder, keeping existing names or updating as needed) ---
from prometheus_client import Counter, Histogram, Gauge
ETL_RECORDS_PROCESSED = Counter('etl_records_processed_total', 'Total records processed', ['source'])
ETL_RUN_DURATION = Histogram('etl_run_duration_seconds', 'ETL run duration', ['source'])
ETL_JOB_STATUS = Gauge('etl_job_status', 'ETL job status (1=Success, 0=Fail)', ['source'])

# --- Checkpoint Logic ---

async def get_checkpoint(session, source_name: str) -> EtlCheckpoint | None:
    result = await session.execute(
        select(EtlCheckpoint).where(EtlCheckpoint.source_name == source_name)
    )
    return result.scalars().first()

async def update_checkpoint(session, source_name: str, status: str, records: int, duration: int, error: str | None = None, last_ts: datetime | None = None):
    ETL_JOB_STATUS.labels(source=source_name).set(1 if status == "success" else 0)
    
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

# --- Loading ---

async def process_source(source_name: str, fetch_func: Callable[[], List[CryptoUnifiedData]]):
    start_time = time.time()
    logger.info("etl_start", source=source_name)
    
    async with AsyncSessionLocal() as session:
        # 1. Checkpoint
        checkpoint = await get_checkpoint(session, source_name)
        last_ingested = checkpoint.last_ingested_timestamp if checkpoint and checkpoint.last_status == "success" else None
        
        try:
            # 2. Extract
            # Run blocking functions in thread
            if asyncio.iscoroutinefunction(fetch_func):
                raw_payload, raw_records = await fetch_func()
            else:
                raw_payload, raw_records = await asyncio.to_thread(fetch_func)
            
            # Save Raw Data (P0.1 Requirement)
            session.add(RawData(source=source_name, payload=raw_payload))
                
            logger.info("fetched_records", source=source_name, count=len(raw_records))

            # 3. Transform & Filter
            new_records = []
            max_ts = last_ingested
            
            processed_count = 0
            total_records = len(raw_records)

            for rec in raw_records:
                try:
                    # Drift Detection (pass raw dict if possible, but here we intentionally get Pydantic model from fetch_func)
                    # For drift detection to work effectively on *schema* changes, we usually need the raw dict.
                    # Our fetch_funcs return List[CryptoUnifiedData] (normalized).
                    # So drift/validation happened inside fetch_func.
                    # We can skip strict drift check here or check against target model fields.
                    if processed_count == 0:
                        detect_drift(rec.model_dump(), CryptoUnifiedData, source_name)

                    # Filter by timestamp if checkpoint exists
                    # Note: UniqueConstraint handles duplicates, but skipping saves DB ops
                    if last_ingested and rec.timestamp and rec.timestamp <= last_ingested:
                        continue
                        
                    new_records.append(rec)
                    processed_count += 1

                    # Chaos Injection
                    if settings.CHAOS_MODE and processed_count > (total_records / 2):
                        raise Exception("CHAOS_MODE_TRIGGERED: Simulated failure mid-stream")
                    
                    if rec.timestamp:
                        # naive vs aware check, ensure aware
                        ts = rec.timestamp
                        if max_ts is None or ts > max_ts:
                            max_ts = ts
                            
                except Exception as e:
                    if "CHAOS" in str(e): raise e
                    logger.warning("validation_error", source=source_name, error=str(e))
                    continue

            # 4. Load (Unified Schema Logic)
            if new_records:
                for data in new_records:
                    # Try to fetch existing record for this ticker/timestamp
                    stmt = select(CryptoMarketData).where(
                        CryptoMarketData.ticker == data.ticker,
                        CryptoMarketData.timestamp == data.timestamp
                    ).with_for_update()
                    result = await session.execute(stmt)
                    existing_record = result.scalars().first()

                    # Data for specific source
                    source_payload = {
                        "price": data.price_usd,
                        "market_cap": data.market_cap,
                        "volume_24h": data.volume_24h
                    }

                    if existing_record:
                        # --- MERGE STRATEGY ---
                        # 1. Update Metadata
                        # Copy existing to avoid mutation issues, though deepcopy implies safety
                        current_meta = dict(existing_record.sources_metadata) if existing_record.sources_metadata else {}
                        current_meta[data.source] = source_payload
                        existing_record.sources_metadata = current_meta

                        # 2. Recalculate Averages
                        total_price = 0.0
                        total_sources = 0
                        
                        for src, metrics in current_meta.items():
                            if "price" in metrics:
                                total_price += metrics["price"]
                                total_sources += 1
                        
                        if total_sources > 0:
                            existing_record.price_usd = total_price / total_sources
                            # We could also average/sum market_cap and volume if needed, 
                            # but Price is the critical unification metric.
                            # For now, let's max() the others or keep latest (simple override)
                            # Or better: Average volume? Volume is usually additive if sources are exchanges, 
                            # but here sources are aggregators (CoinGecko vs CoinPaprika), so Averaging is safer to avoid double counting global volume.
                            existing_record.volume_24h = data.volume_24h # Keep latest/fresh for now or implement avg
                            existing_record.market_cap = data.market_cap

                    else:
                        # --- INSERT NEW ---
                        new_meta = {data.source: source_payload}
                        new_db_obj = CryptoMarketData(
                            ticker=data.ticker,
                            price_usd=data.price_usd,
                            market_cap=data.market_cap,
                            volume_24h=data.volume_24h,
                            timestamp=data.timestamp,
                            sources_metadata=new_meta
                        )
                        session.add(new_db_obj)
            
            await session.commit()
            
            # 5. Update Checkpoint
            duration_ms = int((time.time() - start_time) * 1000)
            await update_checkpoint(session, source_name, "success", len(new_records), duration_ms, last_ts=max_ts)
            await session.commit()
            
            logger.info("etl_success", source=source_name, records=len(new_records), duration_ms=duration_ms)
            
            ETL_RECORDS_PROCESSED.labels(source=source_name).inc(len(new_records))
            ETL_RUN_DURATION.labels(source=source_name).observe(duration_ms / 1000.0)

        except Exception as e:
            await session.rollback()
            logger.error("etl_failure", source=source_name, error=str(e))
            traceback.print_exc()
            duration_ms = int((time.time() - start_time) * 1000)
            await update_checkpoint(session, source_name, "failure", 0, duration_ms, error=str(e))
            await session.commit()

async def run_etl():
    logger.info("pipeline_start", phase=6)
    try:
        # Run concurrently
        await asyncio.gather(
            process_source("coinpaprika", coinpaprika.fetch_data),
            process_source("coingecko", coingecko.fetch_data),
            process_source("csv_upload", csv_ingestor.read_csv_data)
        )
    except Exception as e:
         logger.critical("pipeline_critical_failure", error=str(e))
    logger.info("pipeline_finish")

if __name__ == "__main__":
    asyncio.run(run_etl())
