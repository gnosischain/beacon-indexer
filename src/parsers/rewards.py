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
        reward_rows = []
        
        for reward in rewards_data:
            reward_rows.append({
                "slot": slot,
                "proposer_index": int(reward.get("proposer_index", 0)),
                "total": int(reward.get("total", 0)),
                "attestations": int(reward.get("attestations", 0)),
                "sync_aggregate": int(reward.get("sync_aggregate", 0)),
                "proposer_slashings": int(reward.get("proposer_slashings", 0)),
                "attester_slashings": int(reward.get("attester_slashings", 0))
            })
        
        return {"rewards": reward_rows}
    

