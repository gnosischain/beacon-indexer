import hashlib
import json
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from src.services.beacon_api import BeaconAPI
from src.utils.logger import logger

class BaseLoader(ABC):
    """Base class for all loaders with payload hash support."""
    
    def __init__(self, name: str, beacon_api: BeaconAPI, storage):
        self.name = name
        self.beacon_api = beacon_api
        self.storage = storage  # Generic storage backend (ClickHouse or Parquet)
        self.table_name = f"raw_{name}"
    
    @staticmethod
    def calculate_payload_hash(data: Dict[str, Any]) -> str:
        """Calculate a deterministic hash of the payload for deduplication."""
        try:
            # Convert to JSON with sorted keys for deterministic hashing
            payload_json = json.dumps(data, sort_keys=True, separators=(',', ':'))
            
            # Calculate SHA256 hash and take first 16 characters (64 bits)
            # This gives us 1 in 18 quintillion chance of collision, which is acceptable
            hash_full = hashlib.sha256(payload_json.encode('utf-8')).hexdigest()
            return hash_full[:16]
        except Exception as e:
            logger.error("Error calculating payload hash", error=str(e))
            # Fallback to timestamp-based hash if JSON serialization fails
            return hashlib.sha256(str(data).encode('utf-8')).hexdigest()[:16]
    
    @abstractmethod
    async def fetch_data(self, identifier: Any) -> Optional[Dict[str, Any]]:
        """Fetch raw data from the beacon API."""
        pass
    
    @abstractmethod
    def prepare_row(self, identifier: Any, data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare data for database insertion."""
        pass
    
    def store_data(self, rows: list):
        """Store raw data in storage backend."""
        if rows:
            self.storage.insert_batch(self.table_name, rows)
    
    async def load_single(self, identifier: Any) -> bool:
        """Load a single item."""
        try:
            data = await self.fetch_data(identifier)
            if data is None:
                logger.debug("No data found", loader=self.name, identifier=identifier)
                return True  # Empty slots are normal
            
            row = self.prepare_row(identifier, data)
            self.store_data([row])
            return True
            
        except Exception as e:
            logger.error("Failed to load data", 
                        loader=self.name, 
                        identifier=identifier, 
                        error=str(e))
            return False
    
    async def load_batch(self, identifiers: list) -> int:
        """Load a batch of items."""
        rows = []
        success_count = 0
        
        for identifier in identifiers:
            try:
                data = await self.fetch_data(identifier)
                if data is not None:
                    row = self.prepare_row(identifier, data)
                    rows.append(row)
                success_count += 1
                
            except Exception as e:
                logger.error("Failed to load single item in batch", 
                           loader=self.name, 
                           identifier=identifier, 
                           error=str(e))
        
        if rows:
            self.store_data(rows)
        
        return success_count