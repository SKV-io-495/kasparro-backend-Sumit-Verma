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
    expected_keys = set(model.model_fields.keys())
    
    # Basic Check: Any unexpected keys?
    # Note: We focus on *unexpected* keys or *missing* required keys.
    # For this task, we'll log difference.
    
    # Keys in payload but not in model (Potential new columns)
    new_keys = incoming_keys - expected_keys
    
    if new_keys:
        logger.warning("schema_drift_detected", source=source_name, type="new_keys", keys=list(new_keys))
        
    # Validation of missing keys is handled by Pydantic itself usually, but we can flag it too if we want soft non-blocking warnings.
    # required_keys = {k for k, v in model.model_fields.items() if v.is_required()} # Pydantic v2 logic differs slightly
    # missing = required_keys - incoming_keys 
    
    # Confidence Score (Simple Jaccard Index)
    intersection = len(incoming_keys.intersection(expected_keys))
    union = len(incoming_keys.union(expected_keys))
    
    if union > 0:
        confidence = intersection / union
        if confidence < 0.9:
            logger.warning("low_schema_confidence", source=source_name, confidence=confidence, incoming=list(incoming_keys), expected=list(expected_keys))
