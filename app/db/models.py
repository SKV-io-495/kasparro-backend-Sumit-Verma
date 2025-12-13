from sqlalchemy import Column, Integer, String, DateTime, JSON, TIMESTAMP, Index, Float, UniqueConstraint
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()

class RawData(Base):
    __tablename__ = "raw_data"

    id = Column(Integer, primary_key=True, index=True)
    source = Column(String, index=True)
    payload = Column(JSON)
    ingested_at = Column(DateTime(timezone=True), server_default=func.now())

class CryptoMarketData(Base):
    __tablename__ = "crypto_market_data"

    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String, index=True, nullable=False)
    price_usd = Column(Float, nullable=False)
    market_cap = Column(Float, nullable=True)
    volume_24h = Column(Float, nullable=True)
    # Source is now tracked in metadata to allow unification of multiple sources for the same ticker/time
    sources_metadata = Column(JSON, nullable=True, default={})
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint('ticker', 'timestamp', name='uix_ticker_timestamp'),
    )

class EtlCheckpoint(Base):
    __tablename__ = "etl_checkpoints"

    id = Column(Integer, primary_key=True, index=True)
    source_name = Column(String, unique=True, index=True, nullable=False)
    last_ingested_timestamp = Column(DateTime(timezone=True), nullable=True)
    last_status = Column(String, nullable=False) # success, failure
    records_processed = Column(Integer, default=0)
    run_duration_ms = Column(Integer, default=0)
    error_log = Column(String, nullable=True)

