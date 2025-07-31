from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from src.services.beacon_api import BeaconAPI
from src.services.clickhouse import ClickHouse
from src.utils.logger import logger

class BaseLoader(ABC):
    """Base class for all loaders."""
    
    def __init__(self, name: str, beacon_api: BeaconAPI, clickhouse: ClickHouse):
        self.name = name
        self.beacon_api = beacon_api
        self.clickhouse = clickhouse
        self.table_name = f"raw_{name}"
    
    @abstractmethod
    async def fetch_data(self, identifier: Any) -> Optional[Dict[str, Any]]:
        """Fetch raw data from the beacon API."""
        pass
    
    @abstractmethod
    def prepare_row(self, identifier: Any, data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare data for database insertion."""
        pass
    
    def store_data(self, rows: list):
        """Store raw data in ClickHouse."""
        if rows:
            self.clickhouse.insert_batch(self.table_name, rows)
    
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