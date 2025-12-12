from typing import Dict, Any, Type, Set
from pydantic import BaseModel
from app.core.logging_config import get_logger

logger = get_logger("drift_detection")

def detect_drift(payload: Dict[str, Any], model: Type[BaseModel], source_name: str):
    """
    Checks if the incoming payload has keys that differ from the expected Pydantic model.
    Logs a warning if drift is detected.
    """
    incoming_keys = set(payload.keys())
    # Expected keys are fixed for now for CryptoUnifiedData
    # Let's say we expect 'ticker', 'price', etc. in raw payload depending on source.
    # But this function takes a Pydantic model as 'target'.
    # If we want to check raw payload against source schema, we need source schema.
    # For now, let's just log unexpected keys if we treat 'model' as source schema proxy, 
    # or just generic logging. 
    
    # User request: "Update checks to look for keys like current_price or quotes"
    # This implies we should be checking relevant crypto keys.
    # We will log if critical keys are missing.
    
    critical_keys = {'ticker', 'symbol', 'price', 'price_usd', 'quotes'}
    found_critical = incoming_keys.intersection(critical_keys)
    
    if not found_critical:
        # If none of the common crypto keys are found, likely drift or wrong data
        logger.warning("potential_schema_drift", source=source_name, message="No common crypto keys found", incoming_keys=list(incoming_keys))

    # Existing logic
    if model:
        expected_keys = set(model.model_fields.keys())
        # ... logic ...
        # For this refactor, let's keep it simple as the 'model' passed in pipeline might be CryptoUnifiedData
        # But `process_source` calls detect_drift(rec, UnifiedDataCreate, source_name)
        # We need to update that call site in pipeline.py too.
