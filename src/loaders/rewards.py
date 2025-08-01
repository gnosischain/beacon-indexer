import json
from datetime import datetime
from typing import Dict, Any, Optional
from .base import BaseLoader

class RewardsLoader(BaseLoader):
    """Loader for rewards data with String payload storage."""
    
    def __init__(self, beacon_api, clickhouse):
        super().__init__("rewards", beacon_api, clickhouse)
    
    async def fetch_data(self, slot: int) -> Optional[Dict[str, Any]]:
        """Fetch rewards data from beacon API using slot as state_id."""
        return await self.beacon_api.get_rewards(str(slot))
    
    def prepare_row(self, slot: int, data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare rewards data for database insertion with String payload."""
        return {
            "slot": slot,
            "payload": json.dumps(data),
            "retrieved_at": datetime.now()
        }