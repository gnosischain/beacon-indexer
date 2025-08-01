import json
from datetime import datetime
from typing import Dict, Any, Optional
from .base import BaseLoader

class BlocksLoader(BaseLoader):
    """Loader for beacon blocks with payload hash for fork protection."""
    
    def __init__(self, beacon_api, clickhouse):
        super().__init__("blocks", beacon_api, clickhouse)
    
    async def fetch_data(self, slot: int) -> Optional[Dict[str, Any]]:
        """Fetch block data from beacon API."""
        return await self.beacon_api.get_block(slot)
    
    def prepare_row(self, slot: int, data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare block data for database insertion with payload hash."""
        # Extract block root if available
        block_root = ""
        if "data" in data and "message" in data["data"]:
            block_root = data["data"]["message"].get("state_root", "")
        
        # Calculate payload hash for deduplication
        payload_hash = self.calculate_payload_hash(data)
        
        return {
            "slot": slot,
            "block_root": block_root,
            "payload": json.dumps(data),
            "payload_hash": payload_hash, 
            "retrieved_at": datetime.now()
        }