from app.ingestion.pipeline import run_etl

async def trigger_etl_job():
    """
    Triggers the ETL process.
    """
    # In a real system, this might be a background task (Celery/ARQ).
    # For now, we await it directly or wrap it.
    await run_etl()
