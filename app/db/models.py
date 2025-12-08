from sqlalchemy import Column, Integer, String, DateTime, JSON, TIMESTAMP, Index
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()

class RawData(Base):
    __tablename__ = "raw_data"

    id = Column(Integer, primary_key=True, index=True)
    source = Column(String, index=True)
    payload = Column(JSON)
    ingested_at = Column(DateTime(timezone=True), server_default=func.now())

class UnifiedData(Base):
    __tablename__ = "unified_data"

    id = Column(Integer, primary_key=True, index=True)
    external_id = Column(String, unique=True, index=True, nullable=False)
    name = Column(String, nullable=True)
    timestamp = Column(TIMESTAMP(timezone=True), nullable=True)
    value = Column(String, nullable=True) # Assuming value can be flexible, or float/int if strictly numeric. User said "value" without type, keeping String for generic normalized or Variant. But usually value is numeric. Request says "value", let's assume String/Text to be safe for mixed types or just generic. Or Float. Let's stick to String to be safe for "normalized" data unless specified.
    category = Column(String, index=True, nullable=True)

class EtlCheckpoint(Base):
    __tablename__ = "etl_checkpoints"

    id = Column(Integer, primary_key=True, index=True)
    source_name = Column(String, unique=True, index=True, nullable=False)
    last_ingested_timestamp = Column(DateTime(timezone=True), nullable=True)
    last_status = Column(String, nullable=False) # success, failure
    records_processed = Column(Integer, default=0)
    run_duration_ms = Column(Integer, default=0)
    error_log = Column(String, nullable=True)

