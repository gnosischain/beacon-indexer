from datetime import datetime
from typing import Dict, Any, Optional
from .base import BaseLoader

class BlocksLoader(BaseLoader):
    """Loader for beacon blocks."""
    
    def __init__(self, beacon_api, clickhouse):
        super().__init__("blocks", beacon_api, clickhouse)
    
    async def fetch_data(self, slot: int) -> Optional[Dict[str, Any]]:
        """Fetch block data from beacon API."""
        return await self.beacon_api.get_block(slot)
    
    def prepare_row(self, slot: int, data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare block data for database insertion."""
        # Extract block root if available
        block_root = ""
        if "data" in data and "message" in data["data"]:
            block_root = data["data"]["message"].get("state_root", "")
        
        return {
            "slot": slot,
            "block_root": block_root,
            "payload": data,  # Store as dict, ClickHouse will handle JSON conversion
            "retrieved_at": datetime.now()
        }