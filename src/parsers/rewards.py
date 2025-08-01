import json
from typing import Dict, List, Any
from .base import BaseParser

class RewardsParser(BaseParser):
    """Parser for reward data stored as String payloads."""
    
    def __init__(self):
        super().__init__("rewards")
    
    def parse(self, raw_data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """Parse reward data from String payload into structured format."""
        slot = raw_data.get("slot", 0)
        payload_str = raw_data.get("payload", "{}")
        
        try:
            # Parse JSON string back to dict
            if isinstance(payload_str, str):
                payload = json.loads(payload_str)
            else:
                payload = payload_str
        except (json.JSONDecodeError, TypeError):
            return {}
        
        if "data" not in payload:
            return {}
        
        rewards_data = payload["data"]
        
        # rewards_data is a single dictionary, not a list
        reward_row = {
            "slot": slot,
            "proposer_index": int(rewards_data.get("proposer_index", 0)),
            "total": int(rewards_data.get("total", 0)),
            "attestations": int(rewards_data.get("attestations", 0)),
            "sync_aggregate": int(rewards_data.get("sync_aggregate", 0)),
            "proposer_slashings": int(rewards_data.get("proposer_slashings", 0)),
            "attester_slashings": int(rewards_data.get("attester_slashings", 0))
        }
        
        return {"rewards": [reward_row]}