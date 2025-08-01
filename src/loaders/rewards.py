import json
from datetime import datetime
from typing import Dict, Any, Optional
from .base import BaseLoader

class RewardsLoader(BaseLoader):
    """Loader for rewards data with payload hash for fork protection."""
    
    def __init__(self, beacon_api, clickhouse):
        super().__init__("rewards", beacon_api, clickhouse)
    
    async def fetch_data(self, slot: int) -> Optional[Dict[str, Any]]:
        """Fetch rewards data from beacon API using slot as state_id."""
        return await self.beacon_api.get_rewards(str(slot))
    
    def prepare_row(self, slot: int, data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare rewards data for database insertion with payload hash."""
        # Calculate payload hash for deduplication
        payload_hash = self.calculate_payload_hash(data)
        
        return {
            "slot": slot,
            "payload": json.dumps(data),
            "payload_hash": payload_hash, 
            "retrieved_at": datetime.now()
        }