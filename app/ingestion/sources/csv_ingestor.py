import csv
import logging
from typing import List, Dict, Any
from datetime import datetime, timezone

from app.schemas.crypto import CryptoUnifiedData
from app.core.logging_config import get_logger

logger = get_logger("etl_csv")

def read_csv_data(file_path: str = "data/crypto_sample.csv") -> List[CryptoUnifiedData]:
    """
    Reads CSV with columns: symbol, price, date
    Normalizes to CryptoUnifiedData.
    """
    results = []
    try:
        with open(file_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            # Verify headers
            if not reader.fieldnames:
                logger.warning("csv_empty_headers", path=file_path)
                return []
                
            # Normalize headers to lowercase for safer checking if needed, 
            # but user specified symbol, price, date. We assume they match.
            
            for row in reader:
                try:
                    # Expected columns: symbol, price, date
                    symbol = row.get("symbol")
                    price_str = row.get("price")
                    date_str = row.get("date")
                    
                    if not symbol or not price_str or not date_str:
                        continue
                        
                    try:
                        # Try parsing ISO first, or fallback? 
                        # Sample is 2025-12-11T10:00:00 (ISO-like)
                        ts = datetime.fromisoformat(date_str)
                    except ValueError:
                        # Fallback or error?
                        ts = datetime.now(timezone.utc)
                    
                    # Ensure timezone
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                        
                    data = CryptoUnifiedData(
                        ticker=symbol,
                        price_usd=float(price_str),
                        source="csv_upload",
                        timestamp=ts,
                        # Optional fields
                        market_cap=None,
                        volume_24h=None
                    )
                    results.append(data)
                except Exception as e:
                    logger.warning("csv_row_error", path=file_path, error=str(e), row=row)
                    continue
                    
    except FileNotFoundError:
        logger.warning("csv_not_found", path=file_path)
    except Exception as e:
        logger.error("csv_read_error", path=file_path, error=str(e))
        
    return results
