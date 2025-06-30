from typing import Dict, Any, Optional, List
from datetime import datetime, timezone

def extract_block_info(block_data: Dict) -> Dict[str, Any]:
    """
    Extract standard block information in a normalized format.
    """
    # Extract version and data
    version = block_data.get("version", "phase0")
    data = block_data.get("data", {})
    
    # Check for execution_optimistic and finalized flags
    execution_optimistic = block_data.get("execution_optimistic", False)
    finalized = block_data.get("finalized", False)
    
    # Extract message data
    message = data.get("message", {})
    slot = int(message.get("slot", 0))
    proposer_index = int(message.get("proposer_index", 0))
    parent_root = message.get("parent_root", "")
    state_root = message.get("state_root", "")
    
    # Extract body
    body = message.get("body", {})
    
    # Extract signature
    signature = data.get("signature", "")
    
    # Extract block root (if available)
    block_root = block_data.get("block_root", "")
    
    return {
        "version": version,
        "execution_optimistic": execution_optimistic,
        "finalized": finalized,
        "slot": slot,
        "proposer_index": proposer_index,
        "parent_root": parent_root,
        "state_root": state_root,
        "signature": signature,
        "block_root": block_root,
        "body": body,
        "message": message,
        "data": data
    }

def parse_timestamp(timestamp_str: str) -> datetime:
    """Convert a timestamp string to timezone-naive datetime for ClickHouse."""
    timestamp = int(timestamp_str)
    # Return timezone-naive datetime
    return datetime.fromtimestamp(timestamp).replace(tzinfo=None)


def ensure_list(value: Any) -> List:
    """Ensure a value is a list."""
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]

def extract_execution_timestamp(block_data: Dict) -> Optional[datetime]:
    """Extract execution payload timestamp if available."""
    try:
        version = block_data.get("version", "phase0")
        if version not in ["bellatrix", "capella", "deneb", "electra"]:
            return None
            
        body = block_data.get("data", {}).get("message", {}).get("body", {})
        execution_payload = body.get("execution_payload", {})
        if not execution_payload:
            return None
            
        timestamp_str = execution_payload.get("timestamp", "0")
        if not timestamp_str or timestamp_str == "0":
            return None
            
        return parse_timestamp(timestamp_str)
    except Exception:
        return None